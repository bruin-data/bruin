package sqlengine

// Python numeric semantics needed by the port of sqlglot/optimizer/simplify.py:
// `Literal.to_py()` returns a Python int or a decimal.Decimal, and simplify does arithmetic and
// comparisons on those values (Decimal uses the default context: prec=28, ROUND_HALF_EVEN,
// Emin=-999999, Emax=999999, InvalidOperation/DivisionByZero/Overflow trapped).

import (
	"math/big"
	"strings"
	"unicode"
)

// smpPyError mirrors a Python exception that simplify does not catch (TypeError, OverflowError,
// decimal.InvalidOperation, AttributeError...). It is raised with panic, like the Python raise.
type smpPyError struct{ Type, Msg string }

func (e *smpPyError) Error() string { return e.Type + ": " + e.Msg }

func smpRaise(typ, msg string) { panic(&smpPyError{Type: typ, Msg: msg}) }

const (
	smpDecPrec  = 28
	smpDecEmax  = 999999
	smpDecEmin  = -999999
	smpDecEtiny = smpDecEmin - smpDecPrec + 1
	smpDecEtop  = smpDecEmax - smpDecPrec + 1
)

// smpDecimal mirrors a decimal.Decimal value: (-1)**neg * coef * 10**exp, or a special value.
type smpDecimal struct {
	neg     bool
	coef    *big.Int // >= 0 (nil for specials)
	exp     int64
	special byte // 0: finite, 'I': infinity, 'N': quiet NaN, 'S': signaling NaN
	payload string
}

// smpNum mirrors a Python number produced by Literal.to_py: an int or a Decimal.
type smpNum struct {
	isInt bool
	i     *big.Int
	d     smpDecimal
}

var smpBigTen = big.NewInt(10)

func smpPow10(n int64) *big.Int {
	return new(big.Int).Exp(smpBigTen, big.NewInt(n), nil)
}

func smpIntNum(i *big.Int) smpNum { return smpNum{isInt: true, i: i} }

// smpDigitValue returns the decimal value of a Unicode decimal digit (category Nd), or -1.
func smpDigitValue(r rune) int {
	if r >= '0' && r <= '9' {
		return int(r - '0')
	}
	if !unicode.Is(unicode.Nd, r) {
		return -1
	}
	for _, rg := range unicode.Nd.R16 {
		if rune(rg.Lo) <= r && r <= rune(rg.Hi) {
			return int(r-rune(rg.Lo)) % 10
		}
	}
	for _, rg := range unicode.Nd.R32 {
		if rune(rg.Lo) <= r && r <= rune(rg.Hi) {
			return int(r-rune(rg.Lo)) % 10
		}
	}
	return -1
}

// smpParsePyInt mirrors Python's int(str) (base 10).
func smpParsePyInt(s string) (*big.Int, bool) {
	s = pyStrip(s)
	if s == "" {
		return nil, false
	}
	neg := false
	if s[0] == '+' || s[0] == '-' {
		neg = s[0] == '-'
		s = s[1:]
	}
	if s == "" {
		return nil, false
	}
	var b strings.Builder
	prevUnderscore := true
	for _, r := range s {
		if r == '_' {
			if prevUnderscore {
				return nil, false
			}
			prevUnderscore = true
			continue
		}
		d := smpDigitValue(r)
		if d < 0 {
			return nil, false
		}
		b.WriteByte(byte('0' + d))
		prevUnderscore = false
	}
	if prevUnderscore {
		return nil, false
	}
	i, ok := new(big.Int).SetString(b.String(), 10)
	if !ok {
		return nil, false
	}
	if neg {
		i.Neg(i)
	}
	return i, true
}

// smpParseDecimal mirrors decimal.Decimal(str): underscores are dropped, Unicode digits and
// whitespace are converted, surrounding whitespace is stripped.
func smpParseDecimal(s string) (smpDecimal, bool) {
	var b strings.Builder
	for _, r := range s {
		if r == '_' {
			continue
		}
		if r > 0 && r <= 127 {
			b.WriteRune(r)
			continue
		}
		if pyIsSpaceRune(r) {
			b.WriteByte(' ')
			continue
		}
		d := smpDigitValue(r)
		if d < 0 {
			return smpDecimal{}, false
		}
		b.WriteByte(byte('0' + d))
	}
	t := strings.Trim(b.String(), " \t\n\r\x0b\x0c\x1c\x1d\x1e\x1f")
	if t == "" {
		return smpDecimal{}, false
	}
	var d smpDecimal
	if t[0] == '+' || t[0] == '-' {
		d.neg = t[0] == '-'
		t = t[1:]
	}
	lt := strings.ToLower(t)
	switch {
	case lt == "inf" || lt == "infinity":
		d.special = 'I'
		return d, true
	case strings.HasPrefix(lt, "nan") || strings.HasPrefix(lt, "snan"):
		p := strings.TrimPrefix(strings.TrimPrefix(lt, "s"), "nan")
		for _, c := range p {
			if c < '0' || c > '9' {
				return smpDecimal{}, false
			}
		}
		d.special = 'N'
		if strings.HasPrefix(lt, "s") {
			d.special = 'S'
		}
		d.payload = strings.TrimLeft(p, "0")
		return d, true
	}
	mant, expPart := t, ""
	if i := strings.IndexAny(t, "eE"); i >= 0 {
		mant, expPart = t[:i], t[i+1:]
		if expPart == "" {
			return smpDecimal{}, false
		}
	}
	intPart, fracPart := mant, ""
	if i := strings.IndexByte(mant, '.'); i >= 0 {
		intPart, fracPart = mant[:i], mant[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return smpDecimal{}, false
	}
	for _, c := range intPart + fracPart {
		if c < '0' || c > '9' {
			return smpDecimal{}, false
		}
	}
	var e int64
	if expPart != "" {
		eneg := false
		if expPart[0] == '+' || expPart[0] == '-' {
			eneg = expPart[0] == '-'
			expPart = expPart[1:]
		}
		if expPart == "" {
			return smpDecimal{}, false
		}
		for _, c := range expPart {
			if c < '0' || c > '9' {
				return smpDecimal{}, false
			}
			e = e*10 + int64(c-'0')
			if e > 1<<50 {
				return smpDecimal{}, false
			}
		}
		if eneg {
			e = -e
		}
	}
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		digits = "0"
	}
	d.coef, _ = new(big.Int).SetString(digits, 10)
	d.exp = e - int64(len(fracPart))
	return d, true
}

func (d smpDecimal) isNaN() bool  { return d.special == 'N' || d.special == 'S' }
func (d smpDecimal) isInf() bool  { return d.special == 'I' }
func (d smpDecimal) isZero() bool { return d.special == 0 && d.coef.Sign() == 0 }

func smpDecFromInt(i *big.Int) smpDecimal {
	return smpDecimal{neg: i.Sign() < 0, coef: new(big.Int).Abs(i)}
}

// String mirrors Decimal.__str__ (to-scientific-string).
func (d smpDecimal) String() string {
	sign := ""
	if d.neg {
		sign = "-"
	}
	switch d.special {
	case 'I':
		return sign + "Infinity"
	case 'N':
		return sign + "NaN" + d.payload
	case 'S':
		return sign + "sNaN" + d.payload
	}
	digits := d.coef.String()
	leftdigits := d.exp + int64(len(digits))
	var dotplace int64
	if d.exp <= 0 && leftdigits > -6 {
		dotplace = leftdigits
	} else {
		dotplace = 1
	}
	var intpart, fracpart string
	switch {
	case dotplace <= 0:
		intpart = "0"
		fracpart = "." + strings.Repeat("0", int(-dotplace)) + digits
	case dotplace >= int64(len(digits)):
		intpart = digits + strings.Repeat("0", int(dotplace-int64(len(digits))))
	default:
		intpart = digits[:dotplace]
		fracpart = "." + digits[dotplace:]
	}
	exp := ""
	if leftdigits != dotplace {
		v := leftdigits - dotplace
		if v >= 0 {
			exp = "E+" + big.NewInt(v).String()
		} else {
			exp = "E" + big.NewInt(v).String()
		}
	}
	return sign + intpart + fracpart + exp
}

// smpDecFix mirrors Decimal._fix under the default context.
func smpDecFix(neg bool, coef *big.Int, exp int64) smpDecimal {
	if coef.Sign() == 0 {
		newExp := exp
		if newExp < smpDecEtiny {
			newExp = smpDecEtiny
		}
		if newExp > smpDecEmax {
			newExp = smpDecEmax
		}
		return smpDecimal{neg: neg, coef: new(big.Int), exp: newExp}
	}
	digits := coef.String()
	expMin := int64(len(digits)) + exp - smpDecPrec
	if expMin > smpDecEtop {
		smpRaise("decimal.Overflow", "[<class 'decimal.Overflow'>]")
	}
	if expMin < smpDecEtiny {
		expMin = smpDecEtiny
	}
	if exp < expMin {
		n := int64(len(digits)) + exp - expMin
		if n < 0 {
			digits = "1"
			exp = expMin - 1
			n = 0
		}
		changed := smpRoundHalfEven(digits, int(n))
		keep := digits[:n]
		if keep == "" {
			keep = "0"
		}
		c, _ := new(big.Int).SetString(keep, 10)
		if changed > 0 {
			c.Add(c, big.NewInt(1))
			if len(c.String()) > smpDecPrec {
				c.Quo(c, smpBigTen)
				expMin++
			}
		}
		if expMin > smpDecEtop {
			smpRaise("decimal.Overflow", "[<class 'decimal.Overflow'>]")
		}
		return smpDecimal{neg: neg, coef: c, exp: expMin}
	}
	return smpDecimal{neg: neg, coef: new(big.Int).Set(coef), exp: exp}
}

// smpRoundHalfEven mirrors Decimal._round_half_even on the digit string.
func smpRoundHalfEven(digits string, prec int) int {
	rest := digits[prec:]
	exactHalf := len(rest) > 0 && rest[0] == '5' && strings.Trim(rest[1:], "0") == ""
	if exactHalf && (prec == 0 || strings.ContainsRune("02468", rune(digits[prec-1]))) {
		return -1
	}
	if len(rest) > 0 && strings.ContainsRune("56789", rune(rest[0])) {
		return 1
	}
	if strings.Trim(rest, "0") == "" {
		return 0
	}
	return -1
}

func smpDecInvalid() { smpRaise("decimal.InvalidOperation", "[<class 'decimal.InvalidOperation'>]") }

func smpDecNaNResult(a, b smpDecimal) (smpDecimal, bool) {
	if a.special == 'S' || b.special == 'S' {
		smpDecInvalid()
	}
	if a.isNaN() {
		return a, true
	}
	if b.isNaN() {
		return b, true
	}
	return smpDecimal{}, false
}

func smpDecAdd(a, b smpDecimal) smpDecimal {
	if a.special != 0 || b.special != 0 {
		if r, ok := smpDecNaNResult(a, b); ok {
			return r
		}
		if a.isInf() {
			if b.isInf() && a.neg != b.neg {
				smpDecInvalid()
			}
			return a
		}
		return b
	}
	exp := a.exp
	if b.exp < exp {
		exp = b.exp
	}
	if a.isZero() && b.isZero() {
		return smpDecFix(a.neg && b.neg, new(big.Int), exp)
	}
	if a.isZero() {
		e := max(exp, b.exp-smpDecPrec-1)
		return smpDecFix(b.neg, new(big.Int).Mul(b.coef, smpPow10(b.exp-e)), e)
	}
	if b.isZero() {
		e := max(exp, a.exp-smpDecPrec-1)
		return smpDecFix(a.neg, new(big.Int).Mul(a.coef, smpPow10(a.exp-e)), e)
	}

	// _normalize: give both operands the same exponent (a negligible operand is replaced by a
	// tiny one, which yields the same rounded result).
	type workRep struct {
		neg bool
		i   *big.Int
		exp int64
	}
	op1 := &workRep{a.neg, new(big.Int).Set(a.coef), a.exp}
	op2 := &workRep{b.neg, new(big.Int).Set(b.coef), b.exp}
	tmp, other := op1, op2
	if op1.exp < op2.exp {
		tmp, other = op2, op1
	}
	tmpLen := int64(len(tmp.i.String()))
	otherLen := int64(len(other.i.String()))
	e := tmp.exp + min(-1, tmpLen-smpDecPrec-2)
	if otherLen+other.exp-1 < e {
		other.i = big.NewInt(1)
		other.exp = e
	}
	tmp.i.Mul(tmp.i, smpPow10(tmp.exp-other.exp))
	tmp.exp = other.exp

	var resultNeg bool
	var result *big.Int
	if op1.neg != op2.neg {
		if op1.i.Cmp(op2.i) == 0 {
			// equal and opposite: +0 with ROUND_HALF_EVEN
			return smpDecFix(false, new(big.Int), exp)
		}
		if op1.i.Cmp(op2.i) < 0 {
			op1, op2 = op2, op1
		}
		resultNeg = op1.neg
		result = new(big.Int).Sub(op1.i, op2.i)
	} else {
		resultNeg = op1.neg
		result = new(big.Int).Add(op1.i, op2.i)
	}
	return smpDecFix(resultNeg, result, op1.exp)
}

func smpDecNeg(a smpDecimal) smpDecimal {
	a.neg = !a.neg
	return a
}

func smpDecMul(a, b smpDecimal) smpDecimal {
	neg := a.neg != b.neg
	if a.special != 0 || b.special != 0 {
		if r, ok := smpDecNaNResult(a, b); ok {
			return r
		}
		if (a.isInf() && b.isZero()) || (b.isInf() && a.isZero()) {
			smpDecInvalid()
		}
		return smpDecimal{neg: neg, special: 'I'}
	}
	exp := a.exp + b.exp
	if a.isZero() || b.isZero() {
		return smpDecFix(neg, new(big.Int), exp)
	}
	return smpDecFix(neg, new(big.Int).Mul(a.coef, b.coef), exp)
}

func smpDecDiv(a, b smpDecimal) smpDecimal {
	neg := a.neg != b.neg
	if a.special != 0 || b.special != 0 {
		if r, ok := smpDecNaNResult(a, b); ok {
			return r
		}
		if a.isInf() && b.isInf() {
			smpDecInvalid()
		}
		if a.isInf() {
			return smpDecimal{neg: neg, special: 'I'}
		}
		// x / Infinity
		return smpDecFix(neg, new(big.Int), smpDecEtiny)
	}
	if b.isZero() {
		if a.isZero() {
			smpRaise("decimal.InvalidOperation", "[<class 'decimal.DivisionUndefined'>]")
		}
		smpRaise("decimal.DivisionByZero", "[<class 'decimal.DivisionByZero'>]")
	}
	var coeff *big.Int
	var exp int64
	if a.isZero() {
		exp = a.exp - b.exp
		coeff = new(big.Int)
	} else {
		shift := int64(len(b.coef.String())) - int64(len(a.coef.String())) + smpDecPrec + 1
		exp = a.exp - b.exp - shift
		var rem *big.Int
		if shift >= 0 {
			coeff, rem = new(big.Int).QuoRem(new(big.Int).Mul(a.coef, smpPow10(shift)), b.coef, new(big.Int))
		} else {
			coeff, rem = new(big.Int).QuoRem(a.coef, new(big.Int).Mul(b.coef, smpPow10(-shift)), new(big.Int))
		}
		if rem.Sign() != 0 {
			// result is not exact; adjust to ensure correct rounding
			if new(big.Int).Mod(coeff, big.NewInt(5)).Sign() == 0 {
				coeff.Add(coeff, big.NewInt(1))
			}
		} else {
			// result is exact; get as close to ideal exponent as possible
			idealExp := a.exp - b.exp
			m := new(big.Int)
			for exp < idealExp {
				q, r := new(big.Int).QuoRem(coeff, smpBigTen, m)
				if r.Sign() != 0 {
					break
				}
				coeff = q
				exp++
			}
		}
	}
	return smpDecFix(neg, coeff, exp)
}

// smpDecCmp compares two decimals (ordering comparisons with NaN raise InvalidOperation).
func smpDecCmp(a, b smpDecimal) int {
	if a.isNaN() || b.isNaN() {
		smpDecInvalid()
	}
	if a.isInf() || b.isInf() {
		av, bv := 0, 0
		if a.isInf() {
			av = 1
			if a.neg {
				av = -1
			}
		}
		if b.isInf() {
			bv = 1
			if b.neg {
				bv = -1
			}
		}
		if av != 0 && bv != 0 {
			return av - bv
		}
		if av != 0 {
			return av
		}
		return -bv
	}
	return smpRatOf(a).Cmp(smpRatOf(b))
}

func smpRatOf(d smpDecimal) *big.Rat {
	c := new(big.Int).Set(d.coef)
	if d.neg {
		c.Neg(c)
	}
	if d.exp >= 0 {
		return new(big.Rat).SetInt(c.Mul(c, smpPow10(d.exp)))
	}
	return new(big.Rat).SetFrac(c, smpPow10(-d.exp))
}

func (n smpNum) toDec() smpDecimal {
	if n.isInt {
		return smpDecFromInt(n.i)
	}
	return n.d
}

// String mirrors str(number).
func (n smpNum) String() string {
	if n.isInt {
		return n.i.String()
	}
	return n.d.String()
}

func smpNumAdd(a, b smpNum) smpNum {
	if a.isInt && b.isInt {
		return smpIntNum(new(big.Int).Add(a.i, b.i))
	}
	return smpNum{d: smpDecAdd(a.toDec(), b.toDec())}
}

func smpNumSub(a, b smpNum) smpNum {
	if a.isInt && b.isInt {
		return smpIntNum(new(big.Int).Sub(a.i, b.i))
	}
	return smpNum{d: smpDecAdd(a.toDec(), smpDecNeg(b.toDec()))}
}

func smpNumMul(a, b smpNum) smpNum {
	if a.isInt && b.isInt {
		return smpIntNum(new(big.Int).Mul(a.i, b.i))
	}
	return smpNum{d: smpDecMul(a.toDec(), b.toDec())}
}

// smpNumDiv mirrors a / b where at least one operand is a Decimal.
func smpNumDiv(a, b smpNum) smpNum {
	return smpNum{d: smpDecDiv(a.toDec(), b.toDec())}
}

// smpNumEq mirrors a == b.
func smpNumEq(a, b smpNum) bool {
	if a.isInt && b.isInt {
		return a.i.Cmp(b.i) == 0
	}
	da, db := a.toDec(), b.toDec()
	if da.isNaN() || db.isNaN() {
		if da.special == 'S' || db.special == 'S' {
			smpDecInvalid()
		}
		return false
	}
	return smpDecCmp(da, db) == 0
}

// smpNumCmp mirrors the ordering comparisons of a and b.
func smpNumCmp(a, b smpNum) int {
	if a.isInt && b.isInt {
		return a.i.Cmp(b.i)
	}
	return smpDecCmp(a.toDec(), b.toDec())
}

// smpNumToInt mirrors int(number) (Decimal values are truncated toward zero).
func smpNumToInt(n smpNum) *big.Int {
	if n.isInt {
		return n.i
	}
	d := n.d
	if d.isNaN() {
		panic(&ValueError{Msg: "cannot convert NaN to integer"})
	}
	if d.isInf() {
		smpRaise("OverflowError", "cannot convert Infinity to integer")
	}
	var r *big.Int
	if d.exp >= 0 {
		r = new(big.Int).Mul(d.coef, smpPow10(d.exp))
	} else {
		r = new(big.Int).Quo(d.coef, smpPow10(-d.exp))
	}
	if d.neg {
		r.Neg(r)
	}
	return r
}

// smpLiteralNumber mirrors exp.Literal.number(value) for a Python number.
func smpLiteralNumber(n smpNum) *Expr { return LiteralNumber(n.String()) }

// smpToPy mirrors Expr.to_py: Literal -> int | Decimal | str, Neg -> number * -1, Null -> None,
// Boolean -> bool; anything else raises ValueError.
func smpToPy(e *Expr) any {
	switch {
	case e.IsA(KLiteral):
		if e.IsNumber() {
			s := e.ThisS()
			if i, ok := smpParsePyInt(s); ok {
				return smpIntNum(i)
			}
			d, ok := smpParseDecimal(s)
			if !ok {
				smpRaise("decimal.InvalidOperation", "[<class 'decimal.ConversionSyntax'>]")
			}
			return smpNum{d: d}
		}
		return e.Arg("this")
	case e.IsA(KNeg):
		if e.IsNumber() {
			v, ok := smpToPy(e.This()).(smpNum)
			if !ok {
				smpRaise("TypeError", "bad operand type for unary -")
			}
			return smpNumMul(v, smpIntNum(big.NewInt(-1)))
		}
	case e.IsA(KNull):
		return nil
	case e.IsA(KBoolean):
		return e.Arg("this")
	}
	panic(&ValueError{Msg: "expression cannot be converted to a Python object."})
}

// smpToPyNum returns e.to_py() for a number expression (e.is_number must hold).
func smpToPyNum(e *Expr) smpNum {
	v, ok := smpToPy(e).(smpNum)
	if !ok {
		smpRaise("TypeError", "not a number")
	}
	return v
}

// smpPyInt mirrors int(value) for the values Expr.to_py can return.
func smpPyInt(v any) *big.Int {
	switch x := v.(type) {
	case smpNum:
		return smpNumToInt(x)
	case string:
		i, ok := smpParsePyInt(x)
		if !ok {
			panic(&ValueError{Msg: "invalid literal for int() with base 10: " + pyRepr(x)})
		}
		return i
	case bool:
		if x {
			return big.NewInt(1)
		}
		return big.NewInt(0)
	case nil:
		smpRaise("TypeError", "int() argument must be a string, a bytes-like object or a real number, not 'NoneType'")
	}
	smpRaise("TypeError", "int() argument must be a string, a bytes-like object or a real number")
	return nil
}

// smpPyEq mirrors Python's a == b for the values compared by simplify.
func smpPyEq(a, b any) bool {
	switch x := a.(type) {
	case smpNum:
		if y, ok := b.(smpNum); ok {
			return smpNumEq(x, y)
		}
		return false
	case string:
		y, ok := b.(string)
		return ok && x == y
	case smpDT:
		if y, ok := b.(smpDT); ok {
			return smpDTEq(x, y)
		}
		return false
	}
	return a == b
}

// smpPyCmp mirrors Python's ordering comparisons (raises TypeError for unorderable operands).
func smpPyCmp(a, b any, op string) int {
	switch x := a.(type) {
	case smpNum:
		if y, ok := b.(smpNum); ok {
			return smpNumCmp(x, y)
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y)
		}
	case smpDT:
		if y, ok := b.(smpDT); ok {
			return smpDTCmp(x, y, op)
		}
	}
	smpRaise("TypeError", "'"+op+"' not supported between instances")
	return 0
}

func smpPyLt(a, b any) bool { return smpPyCmp(a, b, "<") < 0 }
func smpPyLe(a, b any) bool { return smpPyCmp(a, b, "<=") <= 0 }
func smpPyGt(a, b any) bool { return smpPyCmp(a, b, ">") > 0 }
func smpPyGe(a, b any) bool { return smpPyCmp(a, b, ">=") >= 0 }

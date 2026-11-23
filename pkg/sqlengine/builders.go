package sqlengine

import (
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

func LiteralString(s string) *Expr { return New(KLiteral, "this", s, "is_string", true) }

// LiteralNumber returns a number literal for an already-formatted number string. A negative
// value becomes Neg wrapping the literal of its absolute value.
func LiteralNumber(s string) *Expr {
	lit := New(KLiteral, "this", s, "is_string", false)
	if v, isInt := lit.toPyNumber(); v != nil {
		if v.Sign() < 0 {
			lit.Set("this", absNumString(v, isInt, s))
			return New(KNeg, "this", lit)
		}
	}
	return lit
}

func LiteralInt(i int) *Expr { return LiteralNumber(strconv.Itoa(i)) }

func absNumString(v *big.Float, isInt bool, orig string) string {
	if isInt {
		i, _ := new(big.Int).SetString(strings.ReplaceAll(strings.TrimSpace(orig), "_", ""), 10)
		if i != nil {
			return new(big.Int).Abs(i).String()
		}
	}
	return strings.TrimPrefix(strings.TrimSpace(orig), "-")
}

// toPyNumber returns the numeric value of a number literal (or a Neg of one) and whether it is
// an integer (as opposed to a decimal). Returns nil if the text is not a valid number.
func (e *Expr) toPyNumber() (*big.Float, bool) {
	if e.IsA(KNeg) {
		v, isInt := e.This().toPyNumber()
		if v == nil {
			return nil, false
		}
		return new(big.Float).Neg(v), isInt
	}
	s := e.ThisS()
	if isPyInt(s) {
		i, ok := new(big.Int).SetString(strings.ReplaceAll(pyStrip(s), "_", ""), 10)
		if ok {
			return new(big.Float).SetInt(i), true
		}
	}
	if f, ok := parsePyDecimal(s); ok {
		return f, false
	}
	return nil, false
}

// isPyInt reports whether s is a base-10 integer: optional surrounding whitespace and sign,
// then digits with single underscores allowed between them.
func isPyInt(s string) bool {
	s = pyStrip(s)
	if s == "" {
		return false
	}
	if s[0] == '+' || s[0] == '-' {
		s = s[1:]
	}
	if s == "" {
		return false
	}
	prevUnderscore := true
	for _, r := range s {
		if r == '_' {
			if prevUnderscore {
				return false
			}
			prevUnderscore = true
			continue
		}
		if r < '0' || r > '9' {
			return false
		}
		prevUnderscore = false
	}
	return !prevUnderscore
}

// pyDecimalRE is the decimal number syntax parsePyDecimal accepts (underscores already removed):
// digits with optional fraction and exponent, Inf/Infinity, and NaN/sNaN with an optional payload.
var pyDecimalRE = regexp.MustCompile(`^[+-]?(((\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?)|[iI][nN][fF]([iI][nN][iI][tT][yY])?|[sS]?[nN][aA][nN]\d*)$`)

func pyDecimalValid(s string) bool {
	return pyDecimalRE.MatchString(pyStrip(strings.ReplaceAll(s, "_", "")))
}

func parsePyDecimal(s string) (*big.Float, bool) {
	if !pyDecimalValid(s) {
		return nil, false
	}
	s = pyStrip(strings.ReplaceAll(s, "_", ""))
	f, _, err := big.ParseFloat(s, 10, 200, big.ToNearestEven)
	if err != nil {
		return nil, false
	}
	return f, true
}

// ToIdentifier returns an identifier for name. A nil quoted quotes the name only when it is not
// a safe unquoted identifier.
func ToIdentifier(name string, quoted *bool) *Expr {
	var q bool
	if quoted != nil {
		q = *quoted
	} else {
		q = !isSafeIdentifier(name)
	}
	return New(KIdentifier, "this", name, "quoted", q)
}

func boolp(b bool) *bool { return &b }

func NewDataType(d DType) *Expr { return New(KDataType, "this", d) }

func (e *Expr) DTypeOf() DType {
	if d, ok := e.Arg("this").(DType); ok {
		return d
	}
	return DT_NONE
}

func Null() *Expr { return New(KNull) }

func Boolean(b bool) *Expr { return New(KBoolean, "this", b) }

func Paren(e *Expr) *Expr { return New(KParen, "this", e) }

func VarExpr(name string) *Expr { return New(KVar, "this", name) }

func Star() *Expr { return New(KStar) }

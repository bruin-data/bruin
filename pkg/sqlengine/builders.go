package sqlengine

import (
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// LiteralString mirrors exp.Literal.string.
func LiteralString(s string) *Expr { return New(KLiteral, "this", s, "is_string", true) }

// LiteralNumber mirrors exp.Literal.number for an already-formatted number string.
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

// LiteralInt mirrors exp.Literal.number(int).
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

// toPyNumber mirrors Literal.to_py for numbers: returns the numeric value and whether it is
// an int (as opposed to a Decimal). Returns nil if the text is not a valid number.
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

// isPyInt reports whether Python's int(s) would succeed (base 10).
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

// pyDecimalRE is the syntax decimal.Decimal(str) accepts (underscores already removed).
var pyDecimalRE = regexp.MustCompile(`^[+-]?(((\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?)|[iI][nN][fF]([iI][nN][iI][tT][yY])?|[sS]?[nN][aA][nN]\d*)$`)

// pyDecimalValid reports whether decimal.Decimal(s) accepts s (else it raises
// InvalidOperation: [<class 'decimal.ConversionSyntax'>]).
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

// ToIdentifier mirrors exp.to_identifier for strings. quoted: nil means auto.
func ToIdentifier(name string, quoted *bool) *Expr {
	q := !SAFE_IDENTIFIER_RE.MatchString(name)
	if quoted != nil {
		q = *quoted
	}
	return New(KIdentifier, "this", name, "quoted", q)
}

func boolp(b bool) *bool { return &b }

// NewDataType mirrors DataType(this=dtype).
func NewDataType(d DType) *Expr { return New(KDataType, "this", d) }

// DType returns args["this"] of a DataType.
func (e *Expr) DTypeOf() DType {
	if d, ok := e.Arg("this").(DType); ok {
		return d
	}
	return DT_NONE
}

// Null mirrors exp.null().
func Null() *Expr { return New(KNull) }

// Boolean mirrors exp.Boolean(this=b).
func Boolean(b bool) *Expr { return New(KBoolean, "this", b) }

// Paren mirrors exp.paren.
func Paren(e *Expr) *Expr { return New(KParen, "this", e) }

// Var mirrors exp.var.
func VarExpr(name string) *Expr { return New(KVar, "this", name) }

// Star mirrors exp.Star().
func Star() *Expr { return New(KStar) }

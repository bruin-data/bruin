package sqlengine

import (
	"math/rand"
	"testing"
	"unicode/utf8"
)

func TestIsSafeIdentifierMatchesRegexp(t *testing.T) {
	t.Parallel()
	check := func(s string) {
		if got, want := isSafeIdentifier(s), SAFE_IDENTIFIER_RE.MatchString(s); got != want {
			t.Fatalf("isSafeIdentifier(%q) = %v, regexp says %v", s, got, want)
		}
	}
	for _, s := range []string{"", "a", "_", "1a", "a1", "a b", "a\n", "a\n\n", "\n", "_\n", "a\r", "ä", "aä", "a١", "a\xff", "a­", "á", "aⅫ", "a²", "A-b", "a.b", "a$"} {
		check(s)
	}
	for r := rune(0); r <= utf8.MaxRune; r++ {
		check("a" + string(r))
		if r < 0x3000 {
			check(string(r))
			check("_" + string(r) + "b")
		}
	}
	rng := rand.New(rand.NewSource(1))
	alphabet := []rune{'a', 'Z', '_', '0', '9', ' ', '\n', '-', 'é', '中', '١', 'Ⅻ', '́', utf8.RuneError}
	for range 200000 {
		n := rng.Intn(6)
		b := make([]rune, n)
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		check(string(b))
		if n > 0 {
			check(string(b) + "\xff")
		}
	}
}

func TestPyUpperEqMatchesPyUpper(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(2))
	alphabet := []rune{'a', 'A', 'z', 's', 'S', 'i', 'I', '_', '1', 'ß', 'ſ', 'ı', 'ﬀ', 'é', 'É', '中', utf8.RuneError}
	words := []string{"", "SELECT", "SS", "S", "I", "FF", "É", "ABC", "SSI"}
	for range 300000 {
		n := rng.Intn(5)
		b := make([]rune, n)
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		s := string(b)
		for _, w := range append(words, pyUpper(s)) {
			if got, want := pyUpperEq(s, w), pyUpper(s) == w; got != want {
				t.Fatalf("pyUpperEq(%q, %q) = %v, want %v", s, w, got, want)
			}
		}
		var buf [4]byte
		if up, ok := upperASCII(buf[:], s); ok && string(up) != pyUpper(s) {
			t.Fatalf("upperASCII(%q) = %q, want %q", s, up, pyUpper(s))
		}
	}
}

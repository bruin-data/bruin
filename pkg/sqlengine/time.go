package sqlengine

// formatTime mirrors sqlglot.time.format_time. Returns ("", false) for None.
func formatTime(s string, mapping map[string]string, tr *trie) (string, bool) {
	if s == "" {
		return "", false
	}
	runes := []rune(s)
	start, end, size := 0, 1, len(runes)
	if tr == nil {
		tr = newTrieFromStrings(mapKeys(mapping)...)
	}
	current := tr
	var chunks []string
	sym := ""
	hasSym := false
	for end <= size {
		chars := string(runes[start:end])
		cr := []rune(chars)
		var result trieResult
		result, current = inTrie(current, []string{string(cr[len(cr)-1])})
		if result == trieFailed {
			if hasSym {
				end--
				chars = sym
				sym, hasSym = "", false
			} else {
				chars = string(cr[0])
				end = start + 1
			}
			start += len([]rune(chars))
			chunks = append(chunks, chars)
			current = tr
		} else if result == trieExists {
			sym, hasSym = chars, true
		}
		end++
		if result != trieFailed && end > size {
			chunks = append(chunks, chars)
		}
	}
	out := ""
	for _, c := range chunks {
		if m, ok := mapping[c]; ok {
			out += m
		} else {
			out += c
		}
	}
	return out, true
}

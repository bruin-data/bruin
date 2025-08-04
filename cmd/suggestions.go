package cmd

import (
	"fmt"
	"os"
)

// levenshteinDistance calculates the Levenshtein distance between two strings
func levenshteinDistance(a, b string) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}

	matrix := make([][]int, len(a)+1)
	for i := range matrix {
		matrix[i] = make([]int, len(b)+1)
	}

	for i := 0; i <= len(a); i++ {
		matrix[i][0] = i
	}
	for j := 0; j <= len(b); j++ {
		matrix[0][j] = j
	}

	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}

			matrix[i][j] = min(
				matrix[i-1][j]+1,      // deletion
				matrix[i][j-1]+1,      // insertion
				matrix[i-1][j-1]+cost, // substitution
			)
		}
	}

	return matrix[len(a)][len(b)]
}

// min returns the minimum of three integers
func min(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

// suggestCommand finds the closest valid command to the given input
func suggestCommand(input string, validCommands []string, threshold int) string {
	bestMatch := ""
	bestDistance := threshold + 1

	for _, cmd := range validCommands {
		distance := levenshteinDistance(input, cmd)
		if distance <= threshold && distance < bestDistance {
			bestDistance = distance
			bestMatch = cmd
		}
	}

	return bestMatch
}

// HandleInvalidCommand prints a suggestion or generic error message
func HandleInvalidCommand(invalidCmd string) {
	validCommands := []string{
		"validate", "run", "render", "lineage", "clean", "format",
		"docs", "init", "environments", "query", "patch", "data-diff",
		"diff", "import", "version", "help", "h",
	}

	suggestion := suggestCommand(invalidCmd, validCommands, 2)
	
	if suggestion != "" {
		fmt.Fprintf(os.Stderr, "Unknown command '%s'. Did you mean '%s'?\n", invalidCmd, suggestion)
		fmt.Fprintf(os.Stderr, "See 'bruin help' for available commands.\n")
	} else {
		fmt.Fprintf(os.Stderr, "Invalid command '%s'. See 'bruin help' for available commands.\n", invalidCmd)
	}
	
	os.Exit(1)
}
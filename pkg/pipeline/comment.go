package pipeline

import (
	"bufio"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	configMarkerString            = "@bruin"
	configMarkerForInlineComments = "@bruin."
)

var commentMarkers = map[string]string{
	".sql": "--",
	".py":  "#",
	".r":   "#",
}

var (
	possiblePrefixesForCommentBlocks = []string{"/*@bruin", "/* @bruin", "/*  @bruin", "/*   @bruin", `"""@bruin`, `""" @bruin`, `"""  @bruin`, `"""   @bruin`, "#@bruin", "# @bruin", "#  @bruin", "#   @bruin"}
	possibleSuffixesForCommentBlocks = []string{"@bruin*/", "@bruin */", "@bruin  */", "@bruin   */", `@bruin"""`, `@bruin """`, `@bruin  """`, `@bruin   """`, "#@bruin", "# @bruin", "#  @bruin", "#   @bruin"}
)

func CreateTaskFromFileComments(fs afero.Fs) TaskCreator {
	return func(filePath string) (*Asset, error) {
		extension := filepath.Ext(filePath)
		commentMarker, ok := commentMarkers[extension]
		if !ok {
			return nil, nil
		}

		file, err := fs.Open(filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to open file %s", filePath)
		}
		defer file.Close()

		if !isEmbeddedYamlComment(file, possiblePrefixesForCommentBlocks) {
			scanner := bufio.NewScanner(file)
			return singleLineCommentsToTask(scanner, commentMarker, filePath)
		}

		return commentedYamlToTask(file, filePath)
	}
}

func isEmbeddedYamlComment(file afero.File, prefixes []string) bool {
	scanner := bufio.NewScanner(file)
	defer func() { _, _ = file.Seek(0, io.SeekStart) }()
	for scanner.Scan() {
		rowText := scanner.Text()
		if rowText == "" || strings.TrimSpace(rowText) == "" {
			continue
		}

		// find the first non-empty row, if it EXACTLY matches one of the prefixes, return true
		// This ensures we only match multiline blocks, not single-line comments like "# @bruin.name: value"
		trimmed := strings.TrimSpace(rowText)
		for _, prefix := range prefixes {
			if trimmed == prefix {
				return true
			}
		}

		// if the first non-empty row doesn't match, return false
		return false
	}

	return false
}

func commentedYamlToTask(file afero.File, filePath string) (*Asset, error) {
	extension := filepath.Ext(filePath)
	commentMarker := commentMarkers[extension]
	rows, commentRowEnd := readUntilComments(file, possiblePrefixesForCommentBlocks, possibleSuffixesForCommentBlocks, commentMarker)
	if rows == "" {
		return nil, &ParseError{"no embedded YAML found in the comments"}
	}

	task, err := ConvertYamlToTask([]byte(rows))
	if err != nil {
		return nil, &ParseError{err.Error()}
	}

	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get absolute path for file %s", filePath)
	}

	scanner := bufio.NewScanner(file)
	content := ""
	for range commentRowEnd {
		scanner.Scan()
	}

	for scanner.Scan() {
		content += scanner.Text() + "\n"
	}

	task.ExecutableFile = ExecutableFile{
		Name:    filepath.Base(filePath),
		Path:    absFilePath,
		Content: strings.TrimSpace(content),
	}

	return task, nil
}

func readUntilComments(file afero.File, prefixes, suffixes []string, commentMarker string) (string, int) {
	scanner := bufio.NewScanner(file)
	defer func() { _, _ = file.Seek(0, io.SeekStart) }()
	rows := ""
	rowCount := 0

	// Check if we're using comment-based multiline blocks (e.g., # @bruin for R)
	// This is only true if the first line is EXACTLY one of the prefixes that start with the comment marker
	isCommentBased := false
	if scanner.Scan() {
		firstLine := strings.TrimSpace(scanner.Text())
		for _, prefix := range prefixes {
			if firstLine == prefix && strings.HasPrefix(prefix, commentMarker) && (commentMarker == "#" || commentMarker == "--") {
				isCommentBased = true
				break
			}
		}
	}
	_, _ = file.Seek(0, io.SeekStart)
	scanner = bufio.NewScanner(file)

	seenPrefix := false

OUTER:
	for scanner.Scan() {
		rowCount += 1

		rowText := scanner.Text()
		trimmed := strings.TrimSpace(rowText)

		// For comment-based blocks where prefix == suffix, track if we've seen the opening
		for _, prefix := range prefixes {
			if trimmed == prefix {
				if !seenPrefix {
					// First occurrence - this is the opening marker
					seenPrefix = true
					continue OUTER
				}
				// Second occurrence - this is the closing marker
				break OUTER
			}
		}

		// For blocks where prefix != suffix (like SQL /* */ or Python """)
		for _, suffix := range suffixes {
			if trimmed == suffix && trimmed != prefixes[0] {
				break OUTER
			}
		}

		// For comment-based blocks, strip the comment marker from each line
		if isCommentBased {
			// Find the comment marker and remove it, preserving indentation after the marker
			idx := strings.Index(rowText, commentMarker)
			if idx >= 0 {
				// Get everything after the comment marker
				afterMarker := rowText[idx+len(commentMarker):]
				// If there's a space immediately after the marker, remove it
				if len(afterMarker) > 0 && afterMarker[0] == ' ' {
					afterMarker = afterMarker[1:]
				}
				rows += afterMarker + "\n"
			} else {
				// If no comment marker found, just add the line as-is (shouldn't happen but be safe)
				rows += rowText + "\n"
			}
		} else {
			rows += rowText + "\n"
		}
	}

	return strings.TrimSpace(rows), rowCount
}

func singleLineCommentsToTask(scanner *bufio.Scanner, commentMarker, filePath string) (*Asset, error) {
	var allRows []string
	var commentRows []string
	for scanner.Scan() {
		rowText := scanner.Text()

		if !strings.HasPrefix(rowText, commentMarker) {
			allRows = append(allRows, rowText)
			continue
		}

		commentValue := strings.TrimSpace(strings.TrimPrefix(rowText, commentMarker))
		if strings.HasPrefix(commentValue, configMarkerForInlineComments) {
			commentRows = append(commentRows, strings.TrimPrefix(commentValue, configMarkerForInlineComments))
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "failed to read file %s", filePath)
	}

	if len(commentRows) == 0 {
		return nil, nil
	}

	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get absolute path for file %s", filePath)
	}

	task, err := commentRowsToTask(commentRows)
	if err != nil {
		return nil, &ParseError{"failed to parse comment formatted task in file " + filePath}
	}

	task.ExecutableFile = ExecutableFile{
		Name:    filepath.Base(filePath),
		Path:    absFilePath,
		Content: strings.TrimSpace(strings.Join(allRows, "\n")),
	}

	return task, nil
}

func commentRowsToTask(commentRows []string) (*Asset, error) {
	task := Asset{
		Parameters:   make(map[string]string),
		Columns:      make([]Column, 0),
		CustomChecks: make([]CustomCheck, 0),
		Secrets:      make([]SecretMapping, 0),
		upstream:     make([]*Asset, 0),
		Upstreams:    make([]Upstream, 0),
	}

	for _, row := range commentRows {
		key, value, found := strings.Cut(row, ":")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		switch key {
		case "name":
			task.Name = value

			continue
		case "description":
			task.Description = value

			continue
		case "type":
			task.Type = AssetType(value)

			continue
		case "connection":
			task.Connection = value

			continue
		case "depends":
			values := strings.Split(value, ",")
			for _, v := range values {
				task.Upstreams = append(task.Upstreams, Upstream{Type: "asset", Value: strings.TrimSpace(v), Columns: make([]DependsColumn, 0), Mode: UpstreamModeFull})
			}

			continue
		case "image":
			task.Image = value

			continue
		case "instance":
			task.Instance = value

			continue
		case "start_date":
			task.StartDate = value

			continue
		case "secrets":
			values := strings.Split(value, ",")
			for _, v := range values {
				secretKey := strings.TrimSpace(v)
				pieces := strings.Split(secretKey, ":")
				injectedKey := secretKey
				if len(pieces) > 1 {
					secretKey = strings.TrimSpace(pieces[0])
					injectedKey = strings.TrimSpace(pieces[1])
				}

				task.Secrets = append(task.Secrets, SecretMapping{
					SecretKey:   secretKey,
					InjectedKey: injectedKey,
				})
			}

			continue
		}

		if strings.HasPrefix(key, "parameters.") {
			parameters := strings.Split(key, ".")
			if len(parameters) != 2 {
				continue
			}

			task.Parameters[parameters[1]] = value
			continue
		}

		if strings.HasPrefix(key, "columns.") {
			// columns.colname.checks: not_null
			columns := strings.Split(key, ".")
			if len(columns) != 3 {
				continue
			}

			err := handleColumnEntry(columns, &task, value)
			if err != nil {
				return nil, err
			}
			continue
		}

		if strings.HasPrefix(key, "materialization.") {
			materializationKeys := strings.Split(key, ".")
			if len(materializationKeys) != 2 {
				continue
			}

			materializationConfigKey := strings.ToLower(materializationKeys[1])
			switch materializationConfigKey {
			case "type":
				task.Materialization.Type = MaterializationType(strings.ToLower(value))
				continue
			case "strategy":
				task.Materialization.Strategy = MaterializationStrategy(strings.ToLower(value))
				continue
			case "partition_by":
				task.Materialization.PartitionBy = value
				continue
			case "incremental_key":
				task.Materialization.IncrementalKey = value
				continue
			case "cluster_by":
				values := strings.Split(value, ",")
				for _, v := range values {
					task.Materialization.ClusterBy = append(task.Materialization.ClusterBy, strings.TrimSpace(v))
				}
				continue
			}
		}
	}

	task.ID = hash(task.Name)

	return &task, nil
}

func handleColumnEntry(columnFields []string, task *Asset, value string) error {
	columnName := columnFields[1]

	columnIndex := -1
	for index, column := range task.Columns {
		if column.Name == columnName {
			columnIndex = index
			break
		}
	}

	if columnIndex == -1 {
		task.Columns = append(task.Columns, Column{
			Name:   columnName,
			Checks: make([]ColumnCheck, 0),
		})
		columnIndex = len(task.Columns) - 1
	}

	trueValue := true

	switch columnFields[2] {
	case "checks":
		checks := strings.Split(value, ",")
		for _, check := range checks {
			task.Columns[columnIndex].Checks = append(task.Columns[columnIndex].Checks, NewColumnCheck(
				task.Name, columnName, strings.TrimSpace(check), ColumnCheckValue{}, &trueValue, "",
			))
		}
	case "type":
		task.Columns[columnIndex].Type = strings.ToLower(strings.TrimSpace(value))
	case "primary_key":
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return errors.Wrapf(err, "failed parsing primary_key for column %s", columnName)
		}
		task.Columns[columnIndex].PrimaryKey = boolValue
	}

	return nil
}

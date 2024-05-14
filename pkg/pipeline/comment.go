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
	configMarker    = "@bruin."
	ParameterLength = 2
	ColumnsLength   = 3
)

var commentMarkers = map[string]string{
	".sql": "--",
	".py":  "#",
}

var (
	possiblePrefixesForCommentBlocks = []string{"/* @bruin", "/*  @bruin", "/*   @bruin", `""" @bruin`, `"""  @bruin`, `"""   @bruin`}
	possibleSuffixesForCommentBlocks = []string{"@bruin */", "@bruin  */", "@bruin   */", `@bruin """`, `@bruin  """`, `@bruin   """`}
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
	scanner.Scan()
	rowText := scanner.Text()

	for _, prefix := range prefixes {
		if strings.HasPrefix(rowText, prefix) {
			return true
		}
	}

	return false
}

func commentedYamlToTask(file afero.File, filePath string) (*Asset, error) {
	rows, commentRowEnd := readUntilComments(file, possiblePrefixesForCommentBlocks, possibleSuffixesForCommentBlocks)
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
	for i := 0; i < commentRowEnd; i++ {
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

func readUntilComments(file afero.File, prefixes, suffixes []string) (string, int) {
	scanner := bufio.NewScanner(file)
	defer func() { _, _ = file.Seek(0, io.SeekStart) }()
	rows := ""
	rowCount := 0

OUTER:
	for scanner.Scan() {
		rowCount += 1

		rowText := scanner.Text()
		for _, suffix := range prefixes {
			if strings.TrimSpace(rowText) == suffix {
				continue OUTER
			}
		}

		for _, suffix := range suffixes {
			if strings.TrimSpace(rowText) == suffix {
				break OUTER
			}
		}

		rows += rowText + "\n"
	}

	return strings.TrimSpace(rows), rowCount
}

func singleLineCommentsToTask(scanner *bufio.Scanner, commentMarker, filePath string) (*Asset, error) {
	var allRows []string
	var commentRows []string
	for scanner.Scan() {
		rowText := scanner.Text()
		allRows = append(allRows, rowText)

		if !strings.HasPrefix(rowText, commentMarker) {
			continue
		}

		commentValue := strings.TrimSpace(strings.TrimPrefix(rowText, commentMarker))
		if strings.HasPrefix(commentValue, configMarker) {
			commentRows = append(commentRows, strings.TrimPrefix(commentValue, configMarker))
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
		Content: strings.Join(allRows, "\n"),
	}

	return task, nil
}

func commentRowsToTask(commentRows []string) (*Asset, error) {
	task := Asset{
		Parameters:   make(map[string]string),
		DependsOn:    []string{},
		Columns:      make([]Column, 0),
		CustomChecks: make([]CustomCheck, 0),
		Secrets:      make([]SecretMapping, 0),
		upstream:     make([]*Asset, 0),
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
				task.DependsOn = append(task.DependsOn, strings.TrimSpace(v))
			}

			continue
		case "image":
			task.Image = value

			continue
		case "instance":
			task.Instance = value

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
			if len(parameters) != ParameterLength {
				continue
			}

			task.Parameters[parameters[1]] = value
			continue
		}

		if strings.HasPrefix(key, "columns.") {
			// columns.colname.checks: not_null
			columns := strings.Split(key, ".")
			if len(columns) != ColumnsLength {
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
			if len(materializationKeys) != ParameterLength {
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

	switch columnFields[2] {
	case "checks":
		checks := strings.Split(value, ",")
		for _, check := range checks {
			task.Columns[columnIndex].Checks = append(task.Columns[columnIndex].Checks, NewColumnCheck(
				task.Name, columnName, strings.TrimSpace(check), ColumnCheckValue{}, true,
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

package query

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type Query struct {
	VariableDefinitions []string
	Query               string
}

func (q Query) ToExplainQuery() string {
	eq := ""
	if len(q.VariableDefinitions) > 0 {
		eq += strings.Join(q.VariableDefinitions, ";\n") + ";\n"
	}

	eq += "EXPLAIN " + q.Query
	if !strings.HasSuffix(eq, ";") {
		eq += ";"
	}

	return eq
}

func (q Query) ToDryRunQuery() string {
	eq := ""
	if len(q.VariableDefinitions) > 0 {
		eq += strings.Join(q.VariableDefinitions, ";\n") + ";\n"
	}

	eq += q.Query
	if !strings.HasSuffix(eq, ";") {
		eq += ";"
	}

	return eq
}

func (q Query) String() string {
	return q.Query
}

var queryCommentRegex = regexp.MustCompile(`(?m)(?s)\/\*.*?\*\/|(^|\s)--.*?\n`)

type renderer interface {
	Render(string) string
}

// FileQuerySplitterExtractor is a regular file extractor, but it splits the queries in the given file into multiple
// instances. For usecases that require EXPLAIN statements, such as validating Snowflake queries, it is not possible
// to EXPLAIN a multi-query string directly, therefore we have to split them.
type FileQuerySplitterExtractor struct {
	Fs       afero.Fs
	Renderer renderer
}

func (f FileQuerySplitterExtractor) ExtractQueriesFromFile(filepath string) ([]*Query, error) {
	contents, err := afero.ReadFile(f.Fs, filepath)
	if err != nil {
		return nil, errors.Wrap(err, "could not read file")
	}

	cleanedUpQueries := queryCommentRegex.ReplaceAllLiteralString(string(contents), "\n")
	cleanedUpQueries = f.Renderer.Render(cleanedUpQueries)

	return splitQueries(cleanedUpQueries), nil
}

func splitQueries(fileContent string) []*Query {
	queries := make([]*Query, 0)
	var sqlVariablesSeenSoFar []string

	for _, query := range strings.Split(fileContent, ";") {
		query = strings.TrimSpace(query)
		if len(query) == 0 {
			continue
		}

		queryLines := strings.Split(query, "\n")
		cleanQueryRows := make([]string, 0, len(queryLines))
		for _, line := range queryLines {
			emptyLine := strings.TrimSpace(line)
			if len(emptyLine) == 0 {
				continue
			}

			cleanQueryRows = append(cleanQueryRows, line)
		}

		cleanQuery := strings.TrimSpace(strings.Join(cleanQueryRows, "\n"))
		lowerCaseVersion := strings.ToLower(cleanQuery)
		if strings.HasPrefix(lowerCaseVersion, "set") || strings.HasPrefix(lowerCaseVersion, "declare") {
			sqlVariablesSeenSoFar = append(sqlVariablesSeenSoFar, cleanQuery)
			continue
		}

		if strings.HasPrefix(lowerCaseVersion, "use") {
			continue
		}

		queries = append(queries, &Query{
			VariableDefinitions: sqlVariablesSeenSoFar,
			Query:               strings.TrimSpace(cleanQuery),
		})
	}

	return queries
}

// WholeFileExtractor is a regular file extractor that returns the whole file content as the query string. It is useful
// for cases where the whole file content can be treated as a single query, such as validating GoogleCloudPlatform queries via dry-run.
type WholeFileExtractor struct {
	Fs       afero.Fs
	Renderer renderer
}

func (f *WholeFileExtractor) ExtractQueriesFromFile(filepath string) ([]*Query, error) {
	contents, err := afero.ReadFile(f.Fs, filepath)
	if err != nil {
		return nil, errors.Wrap(err, "could not read file")
	}

	render := f.Renderer.Render(strings.TrimSpace(string(contents)))
	return []*Query{
		{
			Query: render,
		},
	}, nil
}

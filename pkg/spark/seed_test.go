package spark

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestValidateSparkSeedFileType(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateSparkSeedFileType(""))
	require.NoError(t, validateSparkSeedFileType(" csv "))
	require.NoError(t, validateSparkSeedFileType("CSV"))
	require.EqualError(t, validateSparkSeedFileType("parquet"), `spark.seed only supports CSV files, got "parquet"`)
}

func TestSparkSeedFieldsMatchesDeclaredColumnsCaseInsensitively(t *testing.T) {
	t.Parallel()

	fields := sparkSeedFields(
		[]pipeline.Column{
			{Name: "age", Type: "INT"},
			{Name: "name", Type: "STRING"},
		},
		[]string{" AGE ", "NAME"},
	)

	require.Equal(t, []arrow.Field{
		{Name: "AGE", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "NAME", Type: arrow.BinaryTypes.String, Nullable: true},
	}, fields)
}

func TestSparkArrowTypeWidensTinyInt(t *testing.T) {
	t.Parallel()

	require.Equal(t, arrow.PrimitiveTypes.Int16, sparkArrowType("TINYINT"))
	require.Equal(t, arrow.PrimitiveTypes.Int16, sparkArrowType("BYTE"))
}

func TestReadSeedAcceptsMixedCaseHTTPScheme(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		if _, err := writer.Write([]byte("id,name\n1,Ada\n")); err != nil {
			t.Errorf("failed to write seed response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	seedURL := strings.Replace(server.URL, "http://", "HTTP://", 1)
	data, err := readSeed(t.Context(), &pipeline.Asset{}, seedURL)
	require.NoError(t, err)
	require.Equal(t, "id,name\n1,Ada\n", string(data))
}

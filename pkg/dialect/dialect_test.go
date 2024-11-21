package dialect

import (
	"testing"
)

func TestGetDialectByAssetType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		assetType     string
		wantDialect   string
		wantErr       bool
		expectedError string
	}{
		{
			name:        "Valid BigQuery asset type",
			assetType:   "bq.sql",
			wantDialect: BigQueryDialect,
			wantErr:     false,
		},
		{
			name:        "Valid Snowflake asset type",
			assetType:   "sf.sql",
			wantDialect: SnowflakeDialect,
			wantErr:     false,
		},
		{
			name:        "Valid DuckDB asset type",
			assetType:   "duckdb.sql",
			wantDialect: DuckDBDialect,
			wantErr:     false,
		},
		{
			name:          "Invalid asset type",
			assetType:     "invalid.sql",
			wantDialect:   "",
			wantErr:       true,
			expectedError: "unsupported asset type: invalid.sql",
		},
		{
			name:          "Empty asset type",
			assetType:     "",
			wantDialect:   "",
			wantErr:       true,
			expectedError: "unsupported asset type: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotDialect, err := GetDialectByAssetType(tt.assetType)

			if tt.wantErr {
				if err == nil {
					t.Errorf("GetDialectByAssetType() error = nil, wantErr %v", tt.wantErr)
					return
				}
				if err.Error() != tt.expectedError {
					t.Errorf("GetDialectByAssetType() error = %v, want %v", err.Error(), tt.expectedError)
					return
				}
			} else if err != nil {
				t.Errorf("GetDialectByAssetType() unexpected error = %v", err)
				return
			}

			if gotDialect != tt.wantDialect {
				t.Errorf("GetDialectByAssetType() = %v, want %v", gotDialect, tt.wantDialect)
			}
		})
	}
}

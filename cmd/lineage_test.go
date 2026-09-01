//nolint:paralleltest
package cmd

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type mockPrinter struct {
	buf *bytes.Buffer
	m   sync.Mutex
}

func (m *mockPrinter) Println(a ...interface{}) (int, error) {
	m.m.Lock()
	defer m.m.Unlock()
	return m.buf.Write([]byte(a[0].(string) + "\n"))
}

func (m *mockPrinter) Printf(format string, a ...interface{}) (int, error) {
	m.m.Lock()
	defer m.m.Unlock()
	return fmt.Fprintf(m.buf, format, a...)
}

func (m *mockPrinter) Print(a ...interface{}) (int, error) {
	m.m.Lock()
	defer m.m.Unlock()
	return m.buf.Write([]byte(a[0].(string)))
}

func TestLineageCommand_Run(t *testing.T) {
	t.Parallel()

	type args struct {
		assetPath string
		full      bool
	}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "asset path is empty",
			args: args{
				assetPath: "",
			},
			wantErr: assert.Error,
		},
		{
			name: "failed to find pipeline",
			args: args{
				assetPath: path.AbsPathForTests(t, "./testdata"),
			},
			wantErr: assert.Error,
		},
		{
			name: "failed to find asset",
			args: args{
				assetPath: path.AbsPathForTests(t, "./testdata/simple-pipeline/assets"),
			},
			wantErr: assert.Error,
		},
		{
			name: "generate lineage for no upstream asset",
			args: args{
				assetPath: path.AbsPathForTests(t, "./testdata/simple-pipeline/assets/hello_bq.sql"),
			},
			want: `
Lineage: 'dashboard.hello_bq'

Upstream Dependencies
========================
- hello_python (assets/hello_python.py)

Total: 1


Downstream Dependencies
========================
- nested1 (assets/nested1.sql)

Total: 1
`,
			wantErr: assert.NoError,
		},
		{
			name: "generate full lineage",
			args: args{
				assetPath: path.AbsPathForTests(t, "./testdata/lineage/assets/hello_bq.sql"),
				full:      true,
			},
			want: `
Lineage: 'dashboard.hello_bq'

Upstream Dependencies
========================
- hello_python (assets/nested/hello_python.py)
- bigquery://project_id/dataset_id/table_id (EXTERNAL)

Total: 2


Downstream Dependencies
========================
Asset has no downstream dependencies.
`,
			wantErr: assert.NoError,
		},
		{
			name: "nameless asset gets its name resolved from its path",
			args: args{
				assetPath: path.AbsPathForTests(t, "./testdata/lineage-nameless/assets/nameless.sql"),
			},
			want: `
Lineage: 'nameless'

Upstream Dependencies
========================
Asset has no upstream dependencies.


Downstream Dependencies
========================
- nested.nameless_nested (assets/nested/nameless_nested.sql)

Total: 1
`,
			wantErr: assert.NoError,
		},
		{
			name: "nested nameless asset resolves both its nameless upstream and its named downstream",
			args: args{
				assetPath: path.AbsPathForTests(t, "./testdata/lineage-nameless/assets/nested/nameless_nested.sql"),
			},
			want: `
Lineage: 'nested.nameless_nested'

Upstream Dependencies
========================
- nameless (assets/nameless.sql)

Total: 1


Downstream Dependencies
========================
- dashboard.named (assets/named.sql)

Total: 1
`,
			wantErr: assert.NoError,
		},
		{
			name: "named asset resolves its full upstream through the nameless assets",
			args: args{
				assetPath: path.AbsPathForTests(t, "./testdata/lineage-nameless/assets/named.sql"),
				full:      true,
			},
			want: `
Lineage: 'dashboard.named'

Upstream Dependencies
========================
- nested.nameless_nested (assets/nested/nameless_nested.sql)
- nameless (assets/nameless.sql)

Total: 2


Downstream Dependencies
========================
Asset has no downstream dependencies.
`,
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			mp := &mockPrinter{buf: buf}

			fs := afero.NewOsFs()
			r := &LineageCommand{
				builder:      pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs, nil, nil),
				infoPrinter:  mp,
				errorPrinter: mp,
			}

			res := r.Run(t.Context(), tt.args.assetPath, tt.args.full, "plain", "")
			tt.wantErr(t, res)
			if tt.want != "" {
				want := tt.want
				if runtime.GOOS == osWindows {
					want = strings.ReplaceAll(want, "assets/", "assets\\")
					want = strings.ReplaceAll(want, "nested/", "nested\\")
				}

				assert.Equal(t, want, buf.String())
			}
		})
	}
}

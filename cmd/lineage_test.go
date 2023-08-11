//nolint:paralleltest
package cmd

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

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
	return m.buf.Write([]byte(fmt.Sprintf(format, a...)))
}

func (m *mockPrinter) Print(a ...interface{}) (int, error) {
	m.m.Lock()
	defer m.m.Unlock()
	return m.buf.Write([]byte(a[0].(string)))
}

func TestLineageCommand_Run(t *testing.T) {
	t.Parallel()

	absPath := func(path string) string {
		absolutePath, _ := filepath.Abs(path)
		return absolutePath
	}

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
				assetPath: absPath("./testdata"),
			},
			wantErr: assert.Error,
		},
		{
			name: "failed to find asset",
			args: args{
				assetPath: absPath("./testdata/simple-pipeline/assets"),
			},
			wantErr: assert.Error,
		},
		{
			name: "generate lineage for no upstream asset",
			args: args{
				assetPath: absPath("./testdata/simple-pipeline/assets/hello_bq.sql"),
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
				assetPath: absPath("./testdata/simple-pipeline/assets/hello_bq.sql"),
				full:      true,
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
- nested2 (assets/nested2.sql)

Total: 2
`,
			wantErr: assert.NoError,
		},
	}

	for _, tc := range tests {
		tt := tc
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			mp := &mockPrinter{buf: buf}

			fs := afero.NewOsFs()
			r := &LineageCommand{
				builder:      pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs),
				infoPrinter:  mp,
				errorPrinter: mp,
			}

			res := r.Run(tt.args.assetPath, tt.args.full)
			tt.wantErr(t, res)
			if tt.want != "" {
				assert.Equal(t, tt.want, buf.String())
			}
		})
	}
}

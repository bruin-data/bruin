package python

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockUvInstaller struct {
	mock.Mock
}

func (m *mockUvInstaller) EnsureUvInstalled(ctx context.Context) error {
	called := m.Called(ctx)
	return called.Error(0)
}

func Test_uvPythonRunner_Run(t *testing.T) {
	t.Parallel()

	type fields struct {
		cmd         cmd
		uvInstaller uvInstaller
	}

	repo := &git.Repo{}

	module := "path.to.module"
	tests := []struct {
		name    string
		fields  func() *fields
		execCtx *executionContext
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "if no dependencies are found the basic command should be executed, and error should be propagated",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "~/.bruin/uv",
					Args: []string{"run", "--python", "3.11", "--module", module},
				}).Return(assert.AnError)

				inst := new(mockUvInstaller)
				inst.On("EnsureUvInstalled", mock.Anything).Return(nil)

				return &fields{
					cmd:         cmd,
					uvInstaller: inst,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "",
				asset: &pipeline.Asset{
					Image: "",
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "requirements should be installed",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "~/.bruin/uv",
					Args: []string{"run", "--python", "3.11", "--with-requirements", "/path/to/requirements.txt", "--module", module},
				}).Return(assert.AnError)

				inst := new(mockUvInstaller)
				inst.On("EnsureUvInstalled", mock.Anything).Return(nil)

				return &fields{
					cmd:         cmd,
					uvInstaller: inst,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "/path/to/requirements.txt",
				asset: &pipeline.Asset{
					Image: "",
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "different python versions are supported",
			fields: func() *fields {
				cmd := new(mockCmd)
				cmd.On("Run", mock.Anything, repo, &command{
					Name: "~/.bruin/uv",
					Args: []string{"run", "--python", "3.13", "--with-requirements", "/path/to/requirements.txt", "--module", module},
				}).Return(assert.AnError)

				inst := new(mockUvInstaller)
				inst.On("EnsureUvInstalled", mock.Anything).Return(nil)

				return &fields{
					cmd:         cmd,
					uvInstaller: inst,
				}
			},
			execCtx: &executionContext{
				repo:            repo,
				module:          module,
				requirementsTxt: "/path/to/requirements.txt",
				asset: &pipeline.Asset{
					Image: "python:3.13",
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := tt.fields()
			l := &UvPythonRunner{
				Cmd:         f.cmd,
				UvInstaller: f.uvInstaller,
			}
			tt.wantErr(t, l.Run(context.Background(), tt.execCtx))
		})
	}
}

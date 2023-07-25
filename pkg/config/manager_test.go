package config

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestLoadFromFile(t *testing.T) {
	t.Parallel()

	devEnv := Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountJSON: "{\"key1\": \"value1\"}",
					ServiceAccountFile: "/path/to/service_account.json",
					ProjectID:          "my-project",
				},
			},
			Snowflake: []SnowflakeConnection{
				{
					Name:      "conn2",
					Username:  "user",
					Password:  "pass",
					Account:   "account",
					Region:    "region",
					Role:      "role",
					Database:  "db",
					Schema:    "schema",
					Warehouse: "wh",
				},
			},
		},
	}

	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *Config
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "missing path should error",
			args: args{
				path: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: assert.Error,
		},
		{
			name: "read simple connection",
			args: args{
				path: "testdata/simple.yml",
			},
			want: &Config{
				DefaultEnvironmentName:  "dev",
				SelectedEnvironment:     &devEnv,
				SelectedEnvironmentName: "dev",
				Environments: map[string]Environment{
					"dev": devEnv,
					"prod": {
						Connections: Connections{
							GoogleCloudPlatform: []GoogleCloudPlatformConnection{
								{
									Name:               "conn1",
									ServiceAccountFile: "/path/to/service_account.json",
									ProjectID:          "my-project",
								},
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewReadOnlyFs(afero.NewOsFs())
			got, err := LoadFromFile(fs, tt.args.path)

			tt.wantErr(t, err)
			if tt.want != nil {
				tt.want.fs = fs
				tt.want.path = tt.args.path
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadOrCreate(t *testing.T) {
	t.Parallel()

	configPath := "/some/path/to/config.yml"
	defaultEnv := &Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
			Snowflake: []SnowflakeConnection{},
		},
	}
	existingConfig := &Config{
		path:                    configPath,
		DefaultEnvironmentName:  "dev",
		SelectedEnvironmentName: "dev",
		SelectedEnvironment:     defaultEnv,
		Environments: map[string]Environment{
			"dev": *defaultEnv,
		},
	}

	type args struct {
		fs afero.Fs
	}
	tests := []struct {
		name    string
		setup   func(t *testing.T, args args)
		want    *Config
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "missing path should create",
			want: &Config{
				DefaultEnvironmentName:  "default",
				SelectedEnvironment:     &Environment{},
				SelectedEnvironmentName: "default",
				Environments: map[string]Environment{
					"default": {},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "if any other is returned from the fs then propagate the error",
			setup: func(t *testing.T, args args) {
				err := afero.WriteFile(args.fs, configPath, []byte("some content"), 0o644)
				assert.NoError(t, err)
			},
			wantErr: assert.Error,
		},
		{
			name: "return the config if it exists",
			setup: func(t *testing.T, args args) {
				err := existingConfig.PersistToFs(args.fs)
				assert.NoError(t, err)

				err = afero.WriteFile(args.fs, "/some/path/to/.gitignore", []byte("file1"), 0o644)
				assert.NoError(t, err)
			},
			want:    existingConfig,
			wantErr: assert.NoError,
		},

		{
			name: "return the config if it exists, add to the gitignore",
			setup: func(t *testing.T, args args) {
				err := existingConfig.PersistToFs(args.fs)
				assert.NoError(t, err)
			},
			want:    existingConfig,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := args{
				fs: afero.NewMemMapFs(),
			}

			if tt.setup != nil {
				tt.setup(t, a)
			}

			got, err := LoadOrCreate(a.fs, configPath)
			tt.wantErr(t, err)

			if tt.want != nil {
				assert.EqualExportedValues(t, *tt.want, *got)
			} else {
				assert.Equal(t, tt.want, got)
			}

			exists, err := afero.Exists(a.fs, configPath)
			assert.NoError(t, err)
			assert.True(t, exists)

			if tt.want != nil {
				content, err := afero.ReadFile(a.fs, "/some/path/to/.gitignore")
				assert.NoError(t, err)
				assert.Contains(t, string(content), "config.yml", "config file content: %s", content)
			}
		})
	}
}

func TestConfig_SelectEnvironment(t *testing.T) {
	t.Parallel()

	defaultEnv := &Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/service_account.json",
				},
			},
		},
	}

	prodEnv := &Environment{
		Connections: Connections{
			GoogleCloudPlatform: []GoogleCloudPlatformConnection{
				{
					Name:               "conn1",
					ServiceAccountFile: "/path/to/prod_service_account.json",
				},
			},
		},
	}

	conf := Config{
		DefaultEnvironmentName: "default",
		SelectedEnvironment:    defaultEnv,
		Environments:           map[string]Environment{"default": *defaultEnv, "prod": *prodEnv},
	}

	err := conf.SelectEnvironment("prod")
	assert.NoError(t, err)
	assert.Equal(t, prodEnv, conf.SelectedEnvironment)
	assert.Equal(t, "prod", conf.SelectedEnvironmentName)

	err = conf.SelectEnvironment("non-existing")
	assert.Error(t, err)
	assert.Equal(t, prodEnv, conf.SelectedEnvironment)
	assert.Equal(t, "prod", conf.SelectedEnvironmentName)

	err = conf.SelectEnvironment("default")
	assert.NoError(t, err)
	assert.Equal(t, defaultEnv, conf.SelectedEnvironment)
	assert.Equal(t, "default", conf.SelectedEnvironmentName)
}

package pipeline_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/cmd"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func Test_pipelineBuilder_CreatePipelineFromPath(t *testing.T) {
	t.Parallel()

	type fields struct {
		tasksDirectoryName string
		yamlTaskCreator    pipeline.TaskCreator
		commentTaskCreator pipeline.TaskCreator
	}
	type args struct {
		pathToPipeline string
	}

	asset1 := &pipeline.Asset{
		ID:          "943be81e20336c53de2c8ab40991839ca3b88bcb4f854f03cdbd69825eb369b6",
		URI:         "postgres://host:port/db",
		Name:        "task1",
		Description: "This is a hello world task",
		Type:        "bash",
		ExecutableFile: pipeline.ExecutableFile{
			Name:    "hello.sh",
			Path:    path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/task1/hello.sh"),
			Content: mustRead(t, "testdata/pipeline/first-pipeline/tasks/task1/hello.sh"),
		},
		DefinitionFile: pipeline.TaskDefinitionFile{
			Name: "task.yml",
			Path: path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/task1/task.yml"),
			Type: pipeline.YamlTask,
		},
		Parameters: map[string]string{
			"param1": "value1",
			"param2": "value2",
		},
		Connection: "conn1",
		Secrets:    []pipeline.SecretMapping{},
		Upstreams: []pipeline.Upstream{
			{
				Type:    "asset",
				Value:   "gcs-to-bq",
				Columns: make([]pipeline.DependsColumn, 0),
			},
		},
		Columns:      make([]pipeline.Column, 0),
		CustomChecks: make([]pipeline.CustomCheck, 0),
	}

	asset2 := &pipeline.Asset{
		ID:   "c69409a1840ddb3639a4acbaaec46c238c63b6431cc74ee5254b6dcef7b88c4b",
		Name: "second-task",
		Type: "bq.transfer",
		Parameters: map[string]string{
			"transfer_config_id": "some-uuid",
			"project_id":         "a-new-project-id",
			"location":           "europe-west1",
		},
		ExecutableFile: pipeline.ExecutableFile{
			Name:    "task.yaml",
			Path:    path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/task2/task.yaml"),
			Content: mustReadWithoutReplacement(t, "testdata/pipeline/first-pipeline/tasks/task2/task.yaml"),
		},
		DefinitionFile: pipeline.TaskDefinitionFile{
			Name: "task.yaml",
			Path: path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/task2/task.yaml"),
			Type: pipeline.YamlTask,
		},
		Upstreams:    []pipeline.Upstream{},
		Columns:      make([]pipeline.Column, 0),
		Secrets:      []pipeline.SecretMapping{},
		CustomChecks: make([]pipeline.CustomCheck, 0),
	}

	asset3 := &pipeline.Asset{
		ID:          "21f2fa1b09d584a6b4fe30cd82b4540b769fd777da7c547353386e2930291ef9",
		Name:        "some-python-task",
		Description: "some description goes here",
		Type:        "python",
		ExecutableFile: pipeline.ExecutableFile{
			Name:    "test.py",
			Path:    path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/test.py"),
			Content: "print('hello world')",
		},
		DefinitionFile: pipeline.TaskDefinitionFile{
			Name: "test.py",
			Path: path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/test.py"),
			Type: pipeline.CommentTask,
		},
		Parameters: map[string]string{
			"param1": "first-parameter",
			"param2": "second-parameter",
			"param3": "third-parameter",
		},
		Connection: "first-connection",
		Secrets:    []pipeline.SecretMapping{},
		Upstreams: []pipeline.Upstream{
			{Type: "asset", Value: "task1", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task2", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task3", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task4", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task5", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task3", Columns: make([]pipeline.DependsColumn, 0)},
		},
		Columns:      make([]pipeline.Column, 0),
		CustomChecks: make([]pipeline.CustomCheck, 0),
	}
	asset3.AddUpstream(asset1)
	asset1.AddDownstream(asset3)

	asset4 := &pipeline.Asset{
		ID:          "5812ba61bb0f08ce192bf074c9de21c19355e08cd52e75d008bbff59e5729e5b",
		Name:        "some-sql-task",
		Description: "some description goes here",
		Type:        "bq.sql",
		ExecutableFile: pipeline.ExecutableFile{
			Name:    "test.sql",
			Path:    path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/test.sql"),
			Content: "select *\nfrom foo;",
		},
		DefinitionFile: pipeline.TaskDefinitionFile{
			Name: "test.sql",
			Path: path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/tasks/test.sql"),
			Type: pipeline.CommentTask,
		},
		Parameters: map[string]string{
			"param1": "first-parameter",
			"param2": "second-parameter",
		},
		Connection: "conn2",
		Secrets:    []pipeline.SecretMapping{},
		Upstreams: []pipeline.Upstream{
			{Type: "asset", Value: "task1", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task2", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task3", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task4", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task5", Columns: make([]pipeline.DependsColumn, 0)},
			{Type: "asset", Value: "task3", Columns: make([]pipeline.DependsColumn, 0)},
		},
		Columns:      make([]pipeline.Column, 0),
		CustomChecks: make([]pipeline.CustomCheck, 0),
	}
	asset4.AddUpstream(asset1)
	asset1.AddDownstream(asset4)

	expectedPipeline := &pipeline.Pipeline{
		Name:     "first-pipeline",
		LegacyID: "first-pipeline",
		Schedule: "",
		DefinitionFile: pipeline.DefinitionFile{
			Name: "pipeline.yml",
			Path: path.AbsPathForTests(t, "testdata/pipeline/first-pipeline/pipeline.yml"),
		},

		DefaultConnections: map[string]string{
			"slack":           "slack-connection",
			"gcpConnectionId": "gcp-connection-id-here",
		},
		Retries: 3,
		Assets:  []*pipeline.Asset{asset1, asset2, asset3, asset4},
	}
	fs := afero.NewOsFs()
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *pipeline.Pipeline
		wantErr bool
	}{
		{
			name: "missing path should error",
			fields: fields{
				tasksDirectoryName: "tasks",
			},
			args: args{
				pathToPipeline: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: true,
		},
		{
			name: "should create pipeline from path",
			fields: fields{
				tasksDirectoryName: "tasks",
				commentTaskCreator: pipeline.CreateTaskFromFileComments(fs),
				yamlTaskCreator:    pipeline.CreateTaskFromYamlDefinition(fs),
			},
			args: args{
				pathToPipeline: "testdata/pipeline/first-pipeline",
			},
			want:    expectedPipeline,
			wantErr: false,
		},
		{
			name: "should create pipeline from path even if pipeline.yml is given as a path",
			fields: fields{
				tasksDirectoryName: "tasks",
				commentTaskCreator: pipeline.CreateTaskFromFileComments(fs),
				yamlTaskCreator:    pipeline.CreateTaskFromYamlDefinition(fs),
			},
			args: args{
				pathToPipeline: "testdata/pipeline/first-pipeline/pipeline.yml",
			},
			want:    expectedPipeline,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builderConfig := pipeline.BuilderConfig{
				PipelineFileName:    []string{"pipeline.yml"},
				TasksDirectoryNames: []string{tt.fields.tasksDirectoryName},
				TasksFileSuffixes:   []string{"task.yml", "task.yaml"},
			}

			p := pipeline.NewBuilder(builderConfig, tt.fields.yamlTaskCreator, tt.fields.commentTaskCreator, fs, nil)

			got, err := p.CreatePipelineFromPath(tt.args.pathToPipeline, true)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tt.want == nil {
				return
			}

			assert.Equal(t, tt.want.Name, got.Name)
			assert.Equal(t, tt.want.LegacyID, got.LegacyID)
			assert.Equal(t, tt.want.Schedule, got.Schedule)
			assert.Equal(t, tt.want.DefinitionFile, got.DefinitionFile)
			assert.Equal(t, tt.want.DefaultConnections, got.DefaultConnections)
			assert.Equal(t, tt.want.Retries, got.Retries)

			for i, asset := range tt.want.Assets {
				gotAsset := got.Assets[i]
				assert.EqualExportedValues(t, *asset, *gotAsset)

				gotAssetUpstreams := gotAsset.GetUpstream()
				assert.Equal(t, len(asset.GetUpstream()), len(gotAssetUpstreams))
				for upstreamIdx, u := range asset.GetUpstream() {
					assert.EqualExportedValues(t, *u, *gotAssetUpstreams[upstreamIdx])
				}

				gotAssetDownstreams := gotAsset.GetDownstream()
				assert.Equal(t, len(asset.GetDownstream()), len(gotAssetDownstreams))
				for idx, d := range asset.GetDownstream() {
					assert.EqualExportedValues(t, *d, *gotAssetDownstreams[idx])
				}
			}
		})
	}
}

func TestTask_RelativePathToPipelineRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		task     *pipeline.Asset
		want     string
	}{
		{
			name: "simple relative path returned",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: filepath.Join(string(filepath.Separator), "users", "user1", "pipelines", "pipeline1", "pipeline.yml"),
				},
			},
			task: &pipeline.Asset{
				Name: "test-task",
				DefinitionFile: pipeline.TaskDefinitionFile{
					Path: filepath.Join(string(filepath.Separator), "users", "user1", "pipelines", "pipeline1", "tasks", "task-folder", "task1.sql"),
				},
			},
			want: filepath.Join("tasks", "task-folder", "task1.sql"),
		},
		{
			name: "relative path is calculated even if the tasks are on a parent folder",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: filepath.Join(string(filepath.Separator), "users", "user1", "pipelines", "pipeline1", "pipeline.yml"),
				},
			},
			task: &pipeline.Asset{
				Name: "test-task",
				DefinitionFile: pipeline.TaskDefinitionFile{
					Path: filepath.Join(string(filepath.Separator), "users", "user1", "pipelines", "task-folder", "task1.sql"),
				},
			},
			want: filepath.Join("..", "task-folder", "task1.sql"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.pipeline.RelativeAssetPath(tt.task))
		})
	}
}

func TestPipeline_JsonMarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		pipelinePath    string
		unixJSONPath    string
		windowsJSONPath string
	}{
		{
			name:            "first-pipeline",
			pipelinePath:    "./testdata/pipeline/first-pipeline",
			unixJSONPath:    "./testdata/pipeline/first-pipeline_unix.json",
			windowsJSONPath: "./testdata/pipeline/first-pipeline_windows.json",
		},
		{
			name:            "second-pipeline",
			pipelinePath:    "./testdata/pipeline/second-pipeline",
			unixJSONPath:    "./testdata/pipeline/second-pipeline_unix.json",
			windowsJSONPath: "./testdata/pipeline/second-pipeline_windows.json",
		},
		{
			name:            "pipeline-with-no-assets",
			pipelinePath:    "./testdata/pipeline/pipeline-with-no-assets",
			unixJSONPath:    "./testdata/pipeline/pipeline-with-no-assets_unix.json",
			windowsJSONPath: "./testdata/pipeline/pipeline-with-no-assets_windows.json",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := cmd.DefaultPipelineBuilder.CreatePipelineFromPath(tt.pipelinePath, true)
			require.NoError(t, err)

			got, err := json.Marshal(p)
			require.NoError(t, err)

			dir, err := os.Getwd()
			if err != nil {
				log.Fatal(err)
			}

			path := tt.unixJSONPath
			if runtime.GOOS == "windows" {
				path = tt.windowsJSONPath

				// we do this because of JSON marshalling
				dir = strings.ReplaceAll(dir, "\\", "\\\\")
			}

			// uncomment the line below and run the test once to refresh the data
			// don't forget to comment it out again
			// err = afero.WriteFile(afero.NewOsFs(), path, bytes.ReplaceAll(got, []byte(dir), []byte("__BASEDIR__")), 0o644)

			expected := strings.NewReplacer(
				"__BASEDIR__", dir,
				"__GIT_COMMIT__", currentCommit(),
			).Replace(mustRead(t, path))

			assert.JSONEq(t, expected, string(got))

			var rebuiltPipeline pipeline.Pipeline
			err = json.Unmarshal(got, &rebuiltPipeline)
			require.NoError(t, err)
		})
	}
}

func TestPipeline_HasTaskType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		taskType string
		want     bool
	}{
		{
			name: "existing task type is found",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: "/users/user1/pipelines/pipeline1/pipeline.yml",
				},
				TasksByType: map[pipeline.AssetType][]*pipeline.Asset{
					"type1": {},
					"type2": {},
					"type3": {},
				},
			},
			taskType: "type1",
			want:     true,
		},
		{
			name: "missing task type is returned as false",
			pipeline: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: "/users/user1/pipelines/pipeline1/pipeline.yml",
				},
				TasksByType: map[pipeline.AssetType][]*pipeline.Asset{
					"type1": {},
					"type2": {},
					"type3": {},
				},
			},
			taskType: "some-other-type",
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.pipeline.HasAssetType(pipeline.AssetType(tt.taskType)))
		})
	}
}

func TestAsset_AddUpstream(t *testing.T) {
	t.Parallel()

	asset1 := &pipeline.Asset{Name: "asset1"}
	asset2 := &pipeline.Asset{Name: "asset2"}
	asset3 := &pipeline.Asset{Name: "asset3"}
	asset4 := &pipeline.Asset{Name: "asset4"}

	connect := func(upstream *pipeline.Asset, downstream *pipeline.Asset) {
		t.Helper()
		upstream.AddDownstream(downstream)
		downstream.AddUpstream(upstream)
	}

	connect(asset1, asset2)
	connect(asset2, asset3)
	connect(asset1, asset3)
	connect(asset3, asset4)

	assert.ElementsMatch(t, []*pipeline.Asset{asset2, asset1}, asset3.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset4}, asset3.GetDownstream())

	assert.ElementsMatch(t, []*pipeline.Asset{asset1}, asset2.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset3}, asset2.GetDownstream())

	assert.ElementsMatch(t, []*pipeline.Asset{}, asset1.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset2, asset3}, asset1.GetDownstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset2, asset3, asset4}, asset1.GetFullDownstream())

	assert.ElementsMatch(t, []*pipeline.Asset{asset3}, asset4.GetUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{asset1, asset2, asset3}, asset4.GetFullUpstream())
	assert.ElementsMatch(t, []*pipeline.Asset{}, asset4.GetDownstream())
	assert.ElementsMatch(t, []*pipeline.Asset{}, asset4.GetFullDownstream())
}

func TestPipeline_GetAssetByPath(t *testing.T) {
	t.Parallel()

	fs := afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)
	config := pipeline.BuilderConfig{
		PipelineFileName:    []string{"pipeline.yml"},
		TasksDirectoryNames: []string{"tasks", "assets"},
		TasksFileSuffixes:   []string{"task.yml", "task.yaml"},
	}
	builder := pipeline.NewBuilder(config, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs, nil)
	p, err := builder.CreatePipelineFromPath("./testdata/pipeline/first-pipeline", true)
	require.NoError(t, err)

	asset := p.GetAssetByPath("testdata/pipeline/first-pipeline/tasks/task1/task.yml")
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)

	asset = p.GetAssetByPath("./testdata/pipeline/first-pipeline/tasks/task1/task.yml")
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)

	asset = p.GetAssetByPath(path.AbsPathForTests(t, "./testdata/pipeline/first-pipeline/tasks/task1/task.yml"))
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)

	asset = p.GetAssetByPath(path.AbsPathForTests(t, "../pipeline/testdata/../testdata/pipeline/first-pipeline/tasks/task1/task.yml"))
	assert.NotNil(t, asset)
	assert.Equal(t, "task1", asset.Name)
}

func TestPipeline_GetConnectionNameForAsset(t *testing.T) {
	t.Parallel()

	pipeline1 := &pipeline.Pipeline{
		Name: "pipeline1",
		DefaultConnections: map[string]string{
			"google_cloud_platform": "default-gcp-connection",
			"snowflake":             "default-snowflake-connection",
		},
	}

	t.Run("should return the connection on the asset if exists", func(t *testing.T) {
		t.Parallel()
		found, err := pipeline1.GetConnectionNameForAsset(&pipeline.Asset{
			Type:       "sf.sql",
			Connection: "custom-connection",
		})
		require.NoError(t, err)
		assert.Equal(t, "custom-connection", found)
	})

	t.Run("should return the connection on the asset if exists for ingestr", func(t *testing.T) {
		t.Parallel()

		found, err := pipeline1.GetConnectionNameForAsset(&pipeline.Asset{
			Type:       "ingestr",
			Connection: "connection2",
		})
		require.NoError(t, err)
		assert.Equal(t, "connection2", found)
	})

	t.Run("if ingestr asset doesn't have the connection overridden, should take it from the destination type", func(t *testing.T) {
		t.Parallel()
		found, err := pipeline1.GetConnectionNameForAsset(&pipeline.Asset{
			Type:       "ingestr",
			Parameters: map[string]string{"destination": "bigquery"},
		})
		require.NoError(t, err)
		assert.Equal(t, "default-gcp-connection", found)
	})

	t.Run("should return the right connection if the asset type is known", func(t *testing.T) {
		t.Parallel()
		found, err := pipeline1.GetConnectionNameForAsset(&pipeline.Asset{
			Type: "sf.sql",
		})
		require.NoError(t, err)
		assert.Equal(t, "default-snowflake-connection", found)
	})

	t.Run("should return the default name if the platform is known", func(t *testing.T) {
		t.Parallel()
		found, err := pipeline1.GetConnectionNameForAsset(&pipeline.Asset{
			Name: "asset4",
			Type: "pg.sql",
		})
		require.NoError(t, err)
		assert.Equal(t, "postgres-default", found)
	})
}

func intPointer(i int) *int {
	return &i
}

func floatPointer(f float64) *float64 {
	return &f
}

func stringPointer(s string) *string {
	return &s
}

func boolPointer(b bool) *bool {
	return &b
}

func TestColumnCheckValue_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		jsonFields interface{}
		want       *pipeline.ColumnCheckValue
		wantErr    assert.ErrorAssertionFunc
	}{
		{
			name:       "should unmarshal int array",
			jsonFields: []int{1, 2, 3},
			want: &pipeline.ColumnCheckValue{
				IntArray: &[]int{1, 2, 3},
			},
			wantErr: assert.NoError,
		},
		{
			name:       "should unmarshal string array",
			jsonFields: []string{"1", "2", "3"},
			want: &pipeline.ColumnCheckValue{
				StringArray: &[]string{"1", "2", "3"},
			},
			wantErr: assert.NoError,
		},
		{
			name:       "should unmarshal single int",
			jsonFields: 123,
			want: &pipeline.ColumnCheckValue{
				Int: intPointer(123),
			},
			wantErr: assert.NoError,
		},
		{
			name:       "should unmarshal single float",
			jsonFields: 123.45,
			want: &pipeline.ColumnCheckValue{
				Float: floatPointer(123.45),
			},
			wantErr: assert.NoError,
		},
		{
			name:       "should unmarshal single string",
			jsonFields: "test",
			want: &pipeline.ColumnCheckValue{
				String: stringPointer("test"),
			},
			wantErr: assert.NoError,
		},
		{
			name:       "should unmarshal bool true",
			jsonFields: true,
			want: &pipeline.ColumnCheckValue{
				Bool: boolPointer(true),
			},
			wantErr: assert.NoError,
		},
		{
			name:       "should unmarshal bool false",
			jsonFields: false,
			want: &pipeline.ColumnCheckValue{
				Bool: boolPointer(false),
			},
			wantErr: assert.NoError,
		},
		{
			name:       "should unmarshal nil to nil",
			jsonFields: nil,
			want:       &pipeline.ColumnCheckValue{},
			wantErr:    assert.NoError,
		},
		{
			name:       "should return error for invalid type",
			jsonFields: map[string]interface{}{"invalid": "data"},
			want:       &pipeline.ColumnCheckValue{},
			wantErr:    assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			jsonstr, err := json.Marshal(tt.jsonFields)
			require.NoError(t, err)

			var got pipeline.ColumnCheckValue
			err = json.Unmarshal(jsonstr, &got)
			tt.wantErr(t, err)

			assert.Equal(t, tt.want, &got)
		})
	}
}

func TestColumnCheckValue_MarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ccv     *pipeline.ColumnCheckValue
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "marshal int array",
			ccv: &pipeline.ColumnCheckValue{
				IntArray: &[]int{1, 2, 3},
			},
			want:    "[1,2,3]",
			wantErr: assert.NoError,
		},
		{
			name: "marshal single int",
			ccv: &pipeline.ColumnCheckValue{
				Int: intPointer(123),
			},
			want:    "123",
			wantErr: assert.NoError,
		},
		{
			name: "marshal single float",
			ccv: &pipeline.ColumnCheckValue{
				Float: floatPointer(123.45),
			},
			want:    "123.45",
			wantErr: assert.NoError,
		},
		{
			name: "marshal string array",
			ccv: &pipeline.ColumnCheckValue{
				StringArray: &[]string{"one", "two", "three"},
			},
			want:    "[\"one\",\"two\",\"three\"]",
			wantErr: assert.NoError,
		},
		{
			name: "marshal single string",
			ccv: &pipeline.ColumnCheckValue{
				String: stringPointer("test"),
			},
			want:    "\"test\"",
			wantErr: assert.NoError,
		},
		{
			name: "marshal bool true",
			ccv: &pipeline.ColumnCheckValue{
				Bool: boolPointer(true),
			},
			want:    "true",
			wantErr: assert.NoError,
		},
		{
			name: "marshal bool false",
			ccv: &pipeline.ColumnCheckValue{
				Bool: boolPointer(false),
			},
			want:    "false",
			wantErr: assert.NoError,
		},
		{
			name:    "marshal nil",
			ccv:     &pipeline.ColumnCheckValue{},
			want:    "null",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ccv := &pipeline.ColumnCheckValue{
				IntArray:    tt.ccv.IntArray,
				Int:         tt.ccv.Int,
				Float:       tt.ccv.Float,
				StringArray: tt.ccv.StringArray,
				String:      tt.ccv.String,
				Bool:        tt.ccv.Bool,
			}
			got, err := ccv.MarshalJSON()
			if !tt.wantErr(t, err) {
				return
			}

			assert.Equal(t, tt.want, string(got))
		})
	}
}

func TestPipeline_GetAssetByName(t *testing.T) {
	t.Parallel()

	asset1 := &pipeline.Asset{Name: "asset1"}
	asset2 := &pipeline.Asset{Name: "asset2"}
	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{asset1, asset2},
	}

	assert.Equal(t, asset1, p.GetAssetByName("asset1"))
	assert.Equal(t, asset2, p.GetAssetByName("asset2"))
	assert.Nil(t, p.GetAssetByName("somerandomasset"))
}

func BenchmarkAssetMarshalJSON(b *testing.B) {
	got, err := cmd.DefaultPipelineBuilder.CreatePipelineFromPath("./testdata/pipeline/first-pipeline", true)
	require.NoError(b, err)

	b.ResetTimer()
	for range make([]struct{}, b.N) {
		_, err := json.Marshal(got)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestAsset_Persist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		assetPath    string
		expectedPath string
	}{
		{
			name:         "yaml sql is saved the same",
			assetPath:    path.AbsPathForTests(t, "testdata/persist/embedded_yaml.sql"),
			expectedPath: path.AbsPathForTests(t, "testdata/persist/embedded_yaml.expected.sql"),
		},
		{
			name:         "simple sql is saved the same",
			assetPath:    path.AbsPathForTests(t, "testdata/persist/simple.sql"),
			expectedPath: path.AbsPathForTests(t, "testdata/persist/simple.expected.sql"),
		},
		{
			name:         "python assets are handled",
			assetPath:    path.AbsPathForTests(t, "testdata/persist/basic.py"),
			expectedPath: path.AbsPathForTests(t, "testdata/persist/basic.expected.py"),
		},
		{
			name:         "YAML assets are handled",
			assetPath:    path.AbsPathForTests(t, "testdata/persist/ingestr.asset.yml"),
			expectedPath: path.AbsPathForTests(t, "testdata/persist/ingestr.expected.yml"),
		},
		{
			name:         "multiline strings are handled in YAML",
			assetPath:    path.AbsPathForTests(t, "testdata/persist/big.sql"),
			expectedPath: path.AbsPathForTests(t, "testdata/persist/big.expected.sql"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, err := cmd.DefaultPipelineBuilder.CreateAssetFromFile(tt.assetPath, nil)
			require.NoError(t, err)

			// a.ExecutableFile.Path = tt.expectedPath
			// a.DefinitionFile.Path = tt.expectedPath

			fs := afero.NewMemMapFs()
			err = a.Persist(fs)
			require.NoError(t, err)

			actual, err := afero.ReadFile(fs, a.DefinitionFile.Path)
			require.NoError(t, err)

			expected, err := afero.ReadFile(afero.NewOsFs(), tt.expectedPath)
			require.NoError(t, err)

			expectedStr := strings.ReplaceAll(string(expected), "\r\n", "\n")
			actualStr := strings.ReplaceAll(string(actual), "\r\n", "\n")

			assert.Equal(t, expectedStr, actualStr)
		})
	}
}

func TestClearSpacesAtLineEndings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		content string
		want    string
	}{
		{
			content: "some extra space   ",
			want:    "some extra space",
		},
	}
	for _, tt := range tests {
		t.Run(tt.content, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, pipeline.ClearSpacesAtLineEndings(tt.content), "ClearSpacesAtLineEndings(%v)", tt.content)
		})
	}
}

func BenchmarkClearSpacesAtLineEndings(b *testing.B) {
	content := `
  This asset contains one line per booking, along with the booking's details. It is meant to serve as the single-source-of-truth for the booking entity.  

  ## Primary Terms
  - Booking: the representation of the real-life booking the user had with their coach
  - Booking Status: an enum representing the state of the booking at the time
  - Cancellation Reason: The reason used to cancel the booking, available only for cancelled bookings.

  ## Sample Query
  
	SELECT Organization, count(*)
	FROM dashboard.bookings
	GROUP BY 1
	ORDER BY 2 DESC
`
	for range make([]struct{}, b.N) {
		pipeline.ClearSpacesAtLineEndings(content)
	}
}

func TestDefaultTrueBool_MarshalYAML(t *testing.T) {
	t.Parallel()

	type testStruct struct {
		Field1 string                   `yaml:"field1"`
		Bool   pipeline.DefaultTrueBool `yaml:"bool,omitempty"`
	}

	trueVal := true
	falseVal := false

	tests := []struct {
		name  string
		input testStruct
		want  string
	}{
		{
			name: "true values should be omitted",
			input: testStruct{
				Field1: "field1",
				Bool:   pipeline.DefaultTrueBool{Value: &trueVal},
			},
			want: "field1: field1\n",
		},
		{
			name: "false values should be marshalled",
			input: testStruct{
				Field1: "field1",
				Bool:   pipeline.DefaultTrueBool{Value: &falseVal},
			},
			want: "field1: field1\nbool: false\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buf := bytes.NewBuffer(nil)
			enc := yaml.NewEncoder(buf)
			enc.SetIndent(2)

			err := enc.Encode(tt.input)
			require.NoError(t, err)

			yamlConfig := buf.Bytes()
			assert.YAMLEq(t, tt.want, string(yamlConfig))
		})
	}
}

func TestPipeline_GetCompatibilityHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		expected string
	}{
		{
			name: "single asset with one upstream",
			pipeline: &pipeline.Pipeline{
				Name: "test-pipeline",
				Assets: []*pipeline.Asset{
					{
						Name: "asset1",
						Upstreams: []pipeline.Upstream{
							{Type: "type1", Value: "upstream1"},
						},
					},
				},
			},
			expected: func() string {
				var parts []string
				parts = append(parts, "test-pipeline")
				parts = append(parts, ":asset1{")
				parts = append(parts, ":upstream1:type1:")
				parts = append(parts, "}")
				parts = append(parts, ":")
				hash := sha256.New()
				hash.Write([]byte(strings.Join(parts, "")))
				return hex.EncodeToString(hash.Sum(nil))
			}(),
		},
		{
			name: "multiple assets with multiple upstreams",
			pipeline: &pipeline.Pipeline{
				Name: "complex-pipeline",
				Assets: []*pipeline.Asset{
					{
						Name: "asset1",
						Upstreams: []pipeline.Upstream{
							{Type: "type1", Value: "upstream1"},
							{Type: "type2", Value: "upstream2"},
						},
					},
					{
						Name: "asset2",
						Upstreams: []pipeline.Upstream{
							{Type: "type3", Value: "upstream3"},
						},
					},
				},
			},
			expected: func() string {
				var parts []string
				parts = append(parts, "complex-pipeline")
				parts = append(parts, ":asset1{")
				parts = append(parts, ":upstream1:type1:")
				parts = append(parts, ":upstream2:type2:")
				parts = append(parts, "}")
				parts = append(parts, ":asset2{")
				parts = append(parts, ":upstream3:type3:")
				parts = append(parts, "}")
				parts = append(parts, ":")
				hash := sha256.New()
				hash.Write([]byte(strings.Join(parts, "")))
				return hex.EncodeToString(hash.Sum(nil))
			}(),
		},
		{
			name: "no assets",
			pipeline: &pipeline.Pipeline{
				Name:   "empty-pipeline",
				Assets: []*pipeline.Asset{},
			},
			expected: func() string {
				var parts []string
				parts = append(parts, "empty-pipeline")
				parts = append(parts, ":")
				hash := sha256.New()
				hash.Write([]byte(strings.Join(parts, "")))
				return hex.EncodeToString(hash.Sum(nil))
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actualHash := tt.pipeline.GetCompatibilityHash()
			assert.Equal(t, tt.expected, actualHash)
		})
	}
}

func currentCommit() string {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		panic(err)
	}
	return strings.TrimSpace(string(out))
}

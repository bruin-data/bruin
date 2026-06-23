package odi

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportCreatesPipelineSQLAssetMacrosAndSourceAssets(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	sourceDir := "/odi"
	require.NoError(t, fs.MkdirAll(sourceDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "LSC_LGC_STG.xml"), []byte(logicalSchemaXML("LGC_STG", "STG")), 0o644))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "LSC_LGC_TB.xml"), []byte(logicalSchemaXML("LGC_TB", "TB")), 0o644))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_TEST.xml"), []byte(testScenarioXML()), 0o644))

	result, err := Import(context.Background(), fs, ImportOptions{
		SourcePath:   sourceDir,
		PipelinePath: "/out",
		Connection:   "oracle-prod",
	})
	require.NoError(t, err)

	assert.Equal(t, 3, result.XMLFiles)
	assert.Equal(t, 1, result.Scenarios)
	assert.Equal(t, 1, result.SQLAssets)
	assert.Equal(t, 1, result.SourceAssets)
	assert.Equal(t, 1, result.VariableMacros)
	assert.True(t, result.VariableMacrosWritten)
	assert.False(t, result.VariableMacrosUpdated)
	require.Len(t, result.ControlFlowWarnings, 1)
	assert.Equal(t, "scenario_call", result.ControlFlowWarnings[0].Kind)
	assert.True(t, result.ControlFlowReportWritten)
	assert.True(t, result.PipelineCreated)
	assert.Equal(t, map[string]string{"LGC_STG": "STG", "LGC_TB": "TB"}, result.LogicalSchemaMapping)

	pipelineBytes, err := afero.ReadFile(fs, "/out/pipeline.yml")
	require.NoError(t, err)
	pipelineYAML := string(pipelineBytes)
	assert.Contains(t, pipelineYAML, "name: out")
	assert.Contains(t, pipelineYAML, "oracle: oracle-prod")
	assert.Contains(t, pipelineYAML, "GLOBAL_VAR_ETL_DATE:")
	assert.Contains(t, pipelineYAML, "default: 20250818")

	assetBytes, err := afero.ReadFile(fs, "/out/assets/stg/stg_d_loan_1.sql")
	require.NoError(t, err)
	assetSQL := string(assetBytes)
	assert.Contains(t, assetSQL, "name: stg.stg_d_loan_1")
	assert.Contains(t, assetSQL, "type: oracle.sql")
	assert.Contains(t, assetSQL, "connection: oracle-prod")
	assert.Contains(t, assetSQL, "depends:")
	assert.Contains(t, assetSQL, "- tb.kredi")
	assert.Contains(t, assetSQL, "meta:")
	assert.NotContains(t, assetSQL, "metadata:")
	assert.Contains(t, assetSQL, `"STG"."STG_D_LOAN_1"`)
	assert.Contains(t, assetSQL, `"TB"."KREDI"`)
	assert.Contains(t, assetSQL, "{{ odi_global_var_etl_date() }}")
	assert.Contains(t, assetSQL, "-- ODI command: OdiStartScen -SCEN_NAME=CHILD -SCEN_VERSION=001")

	macrosBytes, err := afero.ReadFile(fs, "/out/macros/odi_variables.sql")
	require.NoError(t, err)
	macrosSQL := string(macrosBytes)
	assert.Contains(t, macrosSQL, "{% macro odi_global_var_etl_date() -%}")
	assert.Contains(t, macrosSQL, "{{ var.GLOBAL_VAR_ETL_DATE }}")

	reportBytes, err := afero.ReadFile(fs, "/out/odi_control_flow_report.yml")
	require.NoError(t, err)
	reportYAML := string(reportBytes)
	assert.Contains(t, reportYAML, `kind: "scenario_call"`)
	assert.Contains(t, reportYAML, "OdiStartScen -SCEN_NAME=CHILD -SCEN_VERSION=001")

	sourceBytes, err := afero.ReadFile(fs, "/out/assets/tb/kredi.asset.yml")
	require.NoError(t, err)
	sourceYAML := string(sourceBytes)
	assert.Contains(t, sourceYAML, "name: tb.kredi")
	assert.Contains(t, sourceYAML, "type: oracle.source")
	assert.Contains(t, sourceYAML, "odi_logical_schema: LGC_TB")
}

func TestImportSkipsExistingAssetsUnlessOverwriteIsSet(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	sourceDir := "/odi"
	require.NoError(t, fs.MkdirAll(sourceDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "LSC_LGC_STG.xml"), []byte(logicalSchemaXML("LGC_STG", "STG")), 0o644))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_TEST.xml"), []byte(testScenarioXML()), 0o644))

	result, err := Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)
	assert.Equal(t, 1, result.SQLAssets)

	result, err = Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)
	assert.Equal(t, 0, result.SQLAssets)
	assert.Equal(t, 2, result.SkippedAssets)

	result, err = Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out", Overwrite: true})
	require.NoError(t, err)
	assert.Equal(t, 1, result.SQLAssets)
	assert.Equal(t, 0, result.SkippedAssets)
}

func TestImportMergesVariablesIntoExistingPipeline(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	sourceDir := "/odi"
	require.NoError(t, fs.MkdirAll(sourceDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_TEST.xml"), []byte(testScenarioXML()), 0o644))
	require.NoError(t, fs.MkdirAll("/out", 0o755))
	require.NoError(t, afero.WriteFile(fs, "/out/pipeline.yml", []byte(`name: existing
variables:
  EXISTING_VAR:
    default: keep-me
    type: string
`), 0o644))

	result, err := Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)
	assert.False(t, result.PipelineCreated)

	pipelineBytes, err := afero.ReadFile(fs, "/out/pipeline.yml")
	require.NoError(t, err)
	pipelineYAML := string(pipelineBytes)
	assert.Contains(t, pipelineYAML, "EXISTING_VAR:")
	assert.Contains(t, pipelineYAML, "default: keep-me")
	assert.Contains(t, pipelineYAML, "GLOBAL_VAR_ETL_DATE:")
	assert.Contains(t, pipelineYAML, "default: 20250818")
}

func TestImportAppendsMissingVariableMacros(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	sourceDir := "/odi"
	require.NoError(t, fs.MkdirAll(sourceDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_VARIABLE_ONLY.xml"), []byte(variableOnlyScenarioXML()), 0o644))
	require.NoError(t, fs.MkdirAll("/out/macros", 0o755))
	require.NoError(t, afero.WriteFile(fs, "/out/macros/odi_variables.sql", []byte(`-- Generated by bruin import odi. Review ODI variable expressions before production use.

{% macro existing_macro() -%}
1
{%- endmacro %}
`), 0o644))

	result, err := Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)
	assert.True(t, result.VariableMacrosWritten)
	assert.True(t, result.VariableMacrosUpdated)
	assert.False(t, result.VariableMacrosSkipped)

	macrosBytes, err := afero.ReadFile(fs, "/out/macros/odi_variables.sql")
	require.NoError(t, err)
	macrosSQL := string(macrosBytes)
	assert.Contains(t, macrosSQL, "{% macro existing_macro() -%}")
	assert.Contains(t, macrosSQL, "{% macro odi_global_var_high_date() -%}")
	assert.Equal(t, 1, strings.Count(macrosSQL, variableMacrosHeader))

	result, err = Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)
	assert.False(t, result.VariableMacrosWritten)
	assert.False(t, result.VariableMacrosUpdated)
	assert.True(t, result.VariableMacrosSkipped)
}

func TestImportMapsScenarioCallsToPipelineControlAssets(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	sourceDir := "/odi"
	require.NoError(t, fs.MkdirAll(sourceDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "LSC_LGC_STG.xml"), []byte(logicalSchemaXML("LGC_STG", "STG")), 0o644))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_PARENT.xml"), []byte(parentCallsChildScenarioXML()), 0o644))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_CHILD.xml"), []byte(childScenarioXML()), 0o644))

	result, err := Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)

	assert.Equal(t, 2, result.Scenarios)
	assert.Equal(t, 2, result.SQLAssets)
	assert.Equal(t, 0, result.SourceAssets)
	assert.Equal(t, 1, result.ControlAssets)
	assert.Equal(t, 1, result.ScenarioCallsResolved)
	require.Len(t, result.ControlFlowWarnings, 1)
	assert.True(t, result.ControlFlowWarnings[0].Resolved)
	assert.Equal(t, "CHILD", result.ControlFlowWarnings[0].TargetScenario)
	assert.Equal(t, "001", result.ControlFlowWarnings[0].TargetVersion)

	callBytes, err := afero.ReadFile(fs, "/out/assets/odi/parent/001_start_child_v001_task_10.asset.yml")
	require.NoError(t, err)
	callYAML := string(callBytes)
	assert.Contains(t, callYAML, "name: odi.parent.001_start_child_v001_task_10")
	assert.Contains(t, callYAML, "type: empty")
	assert.Contains(t, callYAML, "depends:")
	assert.Contains(t, callYAML, "- stg.child_target")
	assert.Contains(t, callYAML, "odi_call_scenario: CHILD")
	assert.Contains(t, callYAML, "odi_call_scenario_version:")

	parentBytes, err := afero.ReadFile(fs, "/out/assets/stg/parent_target.sql")
	require.NoError(t, err)
	parentSQL := string(parentBytes)
	assert.Contains(t, parentSQL, "- stg.child_target")
	assert.Contains(t, parentSQL, "- odi.parent.001_start_child_v001_task_10")

	reportBytes, err := afero.ReadFile(fs, "/out/odi_control_flow_report.yml")
	require.NoError(t, err)
	reportYAML := string(reportBytes)
	assert.Contains(t, reportYAML, "resolved: true")
	assert.Contains(t, reportYAML, `target_scenario: "CHILD"`)
	assert.Contains(t, reportYAML, `target_version: "001"`)
}

func TestGenerateAssetsPreservesLinearOrderAndDuplicateTargetSteps(t *testing.T) {
	t.Parallel()

	project := &Project{
		LogicalSchemaMapping: map[string]string{"LGC_STG": "STG"},
		Scenarios: []Scenario{{
			Name:    "PKG_LINEAR",
			Version: "001",
			Steps: []Step{
				{Number: 1, Name: "LOAD_STAGE", Type: "M", TableName: "STAGE_TABLE", Lschema: "LGC_STG"},
				{Number: 2, Name: "LOAD_STAGE_AGAIN", Type: "M", TableName: "STAGE_TABLE", Lschema: "LGC_STG"},
				{Number: 3, Name: "AUDIT_STAGE", Type: "M", TableName: "AUDIT_TABLE", Lschema: "LGC_STG"},
			},
			Tasks: []Task{
				{
					StepNumber: 1,
					TaskNumber: 10,
					Order:      10,
					Type:       "J",
					DefText:    `insert into <?= odiRef.getObjectName("L", "STAGE_TABLE", "LGC_STG", "D") ?> select 1 from dual`,
				},
				{
					StepNumber: 2,
					TaskNumber: 20,
					Order:      20,
					Type:       "J",
					DefText:    `insert into <?= odiRef.getObjectName("L", "STAGE_TABLE", "LGC_STG", "D") ?> select 2 from dual`,
				},
				{
					StepNumber: 3,
					TaskNumber: 30,
					Order:      30,
					Type:       "J",
					DefText:    `insert into <?= odiRef.getObjectName("L", "AUDIT_TABLE", "LGC_STG", "D") ?> select 1 from dual`,
				},
			},
		}},
	}

	assets := GenerateAssets(project, "/out/assets", "")
	assetsByName := make(map[string]*GeneratedAsset)
	for i := range assets {
		assetsByName[assets[i].Asset.Name] = &assets[i]
	}

	require.Len(t, assets, 3)
	require.Contains(t, assetsByName, "stg.stage_table")
	require.Contains(t, assetsByName, "odi.pkg_linear.002_load_stage_again")
	require.Contains(t, assetsByName, "stg.audit_table")

	assert.Equal(t, filepath.Join("/out/assets", "stg", "stage_table.sql"), assetsByName["stg.stage_table"].Asset.ExecutableFile.Path)
	assert.Equal(t, filepath.Join("/out/assets", "odi", "pkg_linear", "002_load_stage_again.sql"), assetsByName["odi.pkg_linear.002_load_stage_again"].Asset.ExecutableFile.Path)
	assert.Equal(t, []string{"stg.stage_table"}, upstreamValues(assetsByName["odi.pkg_linear.002_load_stage_again"].Asset.Upstreams))
	assert.Equal(t, []string{"odi.pkg_linear.002_load_stage_again"}, upstreamValues(assetsByName["stg.audit_table"].Asset.Upstreams))
}

func TestGenerateAssetsKeepsDependencyOnLaterGeneratedTarget(t *testing.T) {
	t.Parallel()

	project := &Project{
		LogicalSchemaMapping: map[string]string{"LGC_STG": "STG"},
		Scenarios: []Scenario{
			{
				Name: "A_CONSUMER",
				Steps: []Step{
					{Number: 1, Name: "LOAD_CONSUMER", Type: "M", TableName: "CONSUMER_TABLE", Lschema: "LGC_STG"},
				},
				Tasks: []Task{{
					StepNumber: 1,
					TaskNumber: 10,
					Order:      10,
					Type:       "J",
					DefText: `insert into <?= odiRef.getObjectName("L", "CONSUMER_TABLE", "LGC_STG", "D") ?>
select * from <?= odiRef.getObjectName("L", "FUTURE_TABLE", "LGC_STG", "D") ?>`,
				}},
			},
			{
				Name: "B_PRODUCER",
				Steps: []Step{
					{Number: 1, Name: "LOAD_FUTURE", Type: "M", TableName: "FUTURE_TABLE", Lschema: "LGC_STG"},
				},
				Tasks: []Task{{
					StepNumber: 1,
					TaskNumber: 20,
					Order:      20,
					Type:       "J",
					DefText:    `insert into <?= odiRef.getObjectName("L", "FUTURE_TABLE", "LGC_STG", "D") ?> select 1 from dual`,
				}},
			},
		},
	}

	assets := GenerateAssets(project, "/out/assets", "")
	assetsByName := make(map[string]*GeneratedAsset)
	for i := range assets {
		assetsByName[assets[i].Asset.Name] = &assets[i]
	}

	require.Len(t, assets, 2)
	require.Contains(t, assetsByName, "stg.consumer_table")
	require.Contains(t, assetsByName, "stg.future_table")
	assert.Equal(t, []string{"stg.future_table"}, upstreamValues(assetsByName["stg.consumer_table"].Asset.Upstreams))
}

func TestImportSkipsVariableStepsAsStandaloneAssets(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	sourceDir := "/odi"
	require.NoError(t, fs.MkdirAll(sourceDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_VARIABLE_ONLY.xml"), []byte(variableOnlyScenarioXML()), 0o644))

	result, err := Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)

	assert.Equal(t, 1, result.XMLFiles)
	assert.Equal(t, 1, result.Scenarios)
	assert.Equal(t, 0, result.SQLAssets)
	assert.Equal(t, 0, result.SourceAssets)
	assert.Equal(t, 1, result.VariableMacros)

	pipelineBytes, err := afero.ReadFile(fs, "/out/pipeline.yml")
	require.NoError(t, err)
	assert.Contains(t, string(pipelineBytes), "GLOBAL_VAR_HIGH_DATE:")
	assert.Contains(t, string(pipelineBytes), "default: 20991231")

	exists, err := afero.Exists(fs, "/out/assets/odi/pkg_variables/001_var_high_date.sql")
	require.NoError(t, err)
	assert.False(t, exists)

	macrosBytes, err := afero.ReadFile(fs, "/out/macros/odi_variables.sql")
	require.NoError(t, err)
	macrosSQL := string(macrosBytes)
	assert.Contains(t, macrosSQL, "{% macro odi_global_var_high_date() -%}")
	assert.Contains(t, macrosSQL, "20991231")
}

func TestVariableMacroBodyFromTasksWrapsLookupSelect(t *testing.T) {
	t.Parallel()

	body := variableMacroBodyFromTasks([]Task{{
		DefText: `SELECT MAX(CALENDAR_DATE)
FROM DM.D_CALENDAR
WHERE CALENDAR_DATE <= TO_DATE(#GLOBAL.VAR_ETL_DATE, 'YYYYMMDD')`,
	}})

	assert.Contains(t, body, "(SELECT MAX(CALENDAR_DATE)")
	assert.Contains(t, body, "FROM DM.D_CALENDAR")
	assert.Contains(t, body, "{{ odi_global_var_etl_date() }}")
}

func TestImportReportsNonLinearControlFlow(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	sourceDir := "/odi"
	require.NoError(t, fs.MkdirAll(sourceDir, 0o755))
	require.NoError(t, afero.WriteFile(fs, filepath.Join(sourceDir, "SCEN_CONTROL.xml"), []byte(controlFlowScenarioXML()), 0o644))

	result, err := Import(context.Background(), fs, ImportOptions{SourcePath: sourceDir, PipelinePath: "/out"})
	require.NoError(t, err)

	kinds := make([]string, 0, len(result.ControlFlowWarnings))
	for _, warning := range result.ControlFlowWarnings {
		kinds = append(kinds, warning.Kind)
	}
	assert.ElementsMatch(t, []string{"failure_branch", "success_jump", "loop", "variable_operation", "scenario_call"}, kinds)
	assert.True(t, result.ControlFlowReportWritten)

	reportBytes, err := afero.ReadFile(fs, "/out/odi_control_flow_report.yml")
	require.NoError(t, err)
	reportYAML := string(reportBytes)
	assert.Contains(t, reportYAML, `kind: "failure_branch"`)
	assert.Contains(t, reportYAML, `kind: "success_jump"`)
	assert.Contains(t, reportYAML, `kind: "loop"`)
	assert.Contains(t, reportYAML, `kind: "variable_operation"`)
	assert.Contains(t, reportYAML, `kind: "scenario_call"`)
}

func upstreamValues(upstreams []pipeline.Upstream) []string {
	values := make([]string, 0, len(upstreams))
	for _, upstream := range upstreams {
		values = append(values, upstream.Value)
	}
	return values
}

func logicalSchemaXML(logicalName, physicalName string) string {
	return `<?xml version="1.0" encoding="ISO-8859-1"?>
<SunopsisExport>
  <Object class="com.sunopsis.dwg.dbobj.SnpLschema">
    <Field name="LschemaName" type="java.lang.String"><![CDATA[` + logicalName + `]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpFKXRef">
    <Field name="RefKey" type="java.lang.String"><![CDATA[SNP_PSCHEMA.1]]></Field>
    <Field name="RefObjFQName" type="java.lang.String"><![CDATA[Oracle.DWH.DWH.` + physicalName + `]]></Field>
  </Object>
</SunopsisExport>`
}

func variableOnlyScenarioXML() string {
	return `<?xml version="1.0" encoding="ISO-8859-1"?>
<SunopsisExport>
  <Object class="com.sunopsis.dwg.dbobj.SnpScen">
    <Field name="ScenName" type="java.lang.String"><![CDATA[PKG_VARIABLES]]></Field>
    <Field name="ScenNo" type="com.sunopsis.sql.DbInt"><![CDATA[10]]></Field>
    <Field name="ScenVersion" type="java.lang.String"><![CDATA[001]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpVarScen">
    <Field name="VarName" type="java.lang.String"><![CDATA[GLOBAL.VAR_HIGH_DATE]]></Field>
    <Field name="VarDatatype" type="java.lang.String"><![CDATA[N]]></Field>
    <Field name="DefN" type="com.sunopsis.sql.DbInt"><![CDATA[20991231]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[VAR_HIGH_DATE]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[V]]></Field>
    <Field name="VarName" type="java.lang.String"><![CDATA[GLOBAL.VAR_HIGH_DATE]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenTask">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="ScenTaskNo" type="com.sunopsis.sql.DbInt"><![CDATA[2]]></Field>
    <Field name="TaskName1" type="java.lang.String"><![CDATA[Variable]]></Field>
    <Field name="TaskName2" type="java.lang.String"><![CDATA[VAR_HIGH_DATE]]></Field>
    <Field name="TaskType" type="java.lang.String"><![CDATA[V]]></Field>
    <Field name="DefTxt" type="java.lang.String"><![CDATA[SELECT 20991231 FROM DUAL]]></Field>
  </Object>
</SunopsisExport>`
}

func controlFlowScenarioXML() string {
	return `<?xml version="1.0" encoding="ISO-8859-1"?>
<SunopsisExport>
  <Object class="com.sunopsis.dwg.dbobj.SnpScen">
    <Field name="ScenName" type="java.lang.String"><![CDATA[PKG_CONTROL]]></Field>
    <Field name="ScenNo" type="com.sunopsis.sql.DbInt"><![CDATA[20]]></Field>
    <Field name="ScenVersion" type="java.lang.String"><![CDATA[001]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[MAP_SKIP]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[M]]></Field>
    <Field name="OkNextStep" type="com.sunopsis.sql.DbInt"><![CDATA[3]]></Field>
    <Field name="KoNextStep" type="com.sunopsis.sql.DbInt"><![CDATA[4]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[2]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[VAR_COMPARE]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[V]]></Field>
    <Field name="VarName" type="java.lang.String"><![CDATA[GLOBAL.VAR_FLAG]]></Field>
    <Field name="VarOp" type="java.lang.String"><![CDATA[!=]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[3]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[MAP_LOOP]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[M]]></Field>
    <Field name="OkNextStep" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[4]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[START_CHILD]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[SE]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenTask">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[4]]></Field>
    <Field name="ScenTaskNo" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="TaskName1" type="java.lang.String"><![CDATA[Oracle Data Integrator Command]]></Field>
    <Field name="TaskType" type="java.lang.String"><![CDATA[S]]></Field>
    <Field name="DefTxt" type="java.lang.String"><![CDATA[OdiStartScen -SCEN_NAME=CHILD -SCEN_VERSION=001]]></Field>
  </Object>
</SunopsisExport>`
}

func testScenarioXML() string {
	return `<?xml version="1.0" encoding="ISO-8859-1"?>
<SunopsisExport>
  <Object class="com.sunopsis.dwg.dbobj.SnpScen">
    <Field name="ScenName" type="java.lang.String"><![CDATA[PKG_D_LOAN_STG_1]]></Field>
    <Field name="ScenNo" type="com.sunopsis.sql.DbInt"><![CDATA[63]]></Field>
    <Field name="ScenVersion" type="java.lang.String"><![CDATA[001]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpVarScen">
    <Field name="VarName" type="java.lang.String"><![CDATA[GLOBAL.VAR_ETL_DATE]]></Field>
    <Field name="VarDatatype" type="java.lang.String"><![CDATA[N]]></Field>
    <Field name="DefN" type="com.sunopsis.sql.DbInt"><![CDATA[20250818]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[2]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[MAP_STG_D_LOAN_1]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[M]]></Field>
    <Field name="TableName" type="java.lang.String"><![CDATA[STG_D_LOAN_1]]></Field>
    <Field name="LschemaName" type="java.lang.String"><![CDATA[LGC_STG]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenTask">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[2]]></Field>
    <Field name="OrdTrt" type="com.sunopsis.sql.DbInt"><![CDATA[80]]></Field>
    <Field name="ScenTaskNo" type="com.sunopsis.sql.DbInt"><![CDATA[80]]></Field>
    <Field name="TaskName1" type="java.lang.String"><![CDATA[Insert new rows]]></Field>
    <Field name="TaskName2" type="java.lang.String"><![CDATA[IKM Oracle]]></Field>
    <Field name="TaskType" type="java.lang.String"><![CDATA[J]]></Field>
    <Field name="DefLschemaName" type="java.lang.String"><![CDATA[LGC_STG]]></Field>
    <Field name="DefTxt" type="java.lang.String"><![CDATA[
insert into <?= odiRef.getObjectName("L", "STG_D_LOAN_1", "LGC_STG", "D") ?>
select *
from <?= odiRef.getObjectName("L", "KREDI", "LGC_TB", "D") ?>
where AC_TAR <= TO_DATE(#GLOBAL.VAR_ETL_DATE,'YYYYMMDD')
    ]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenTask">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[2]]></Field>
    <Field name="OrdTrt" type="com.sunopsis.sql.DbInt"><![CDATA[90]]></Field>
    <Field name="ScenTaskNo" type="com.sunopsis.sql.DbInt"><![CDATA[90]]></Field>
    <Field name="TaskName1" type="java.lang.String"><![CDATA[Start child scenario]]></Field>
    <Field name="TaskType" type="java.lang.String"><![CDATA[S]]></Field>
    <Field name="DefTxt" type="java.lang.String"><![CDATA[OdiStartScen -SCEN_NAME=CHILD -SCEN_VERSION=001]]></Field>
  </Object>
</SunopsisExport>`
}

func parentCallsChildScenarioXML() string {
	return `<?xml version="1.0" encoding="ISO-8859-1"?>
<SunopsisExport>
  <Object class="com.sunopsis.dwg.dbobj.SnpScen">
    <Field name="ScenName" type="java.lang.String"><![CDATA[PARENT]]></Field>
    <Field name="ScenNo" type="com.sunopsis.sql.DbInt"><![CDATA[101]]></Field>
    <Field name="ScenVersion" type="java.lang.String"><![CDATA[001]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[START_CHILD]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[SE]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenTask">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="OrdTrt" type="com.sunopsis.sql.DbInt"><![CDATA[10]]></Field>
    <Field name="ScenTaskNo" type="com.sunopsis.sql.DbInt"><![CDATA[10]]></Field>
    <Field name="TaskName1" type="java.lang.String"><![CDATA[Oracle Data Integrator Command]]></Field>
    <Field name="TaskType" type="java.lang.String"><![CDATA[S]]></Field>
    <Field name="DefTxt" type="java.lang.String"><![CDATA[OdiStartScen -SCEN_NAME=CHILD -SCEN_VERSION=001]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[2]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[MAP_PARENT]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[M]]></Field>
    <Field name="TableName" type="java.lang.String"><![CDATA[PARENT_TARGET]]></Field>
    <Field name="LschemaName" type="java.lang.String"><![CDATA[LGC_STG]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenTask">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[2]]></Field>
    <Field name="OrdTrt" type="com.sunopsis.sql.DbInt"><![CDATA[80]]></Field>
    <Field name="ScenTaskNo" type="com.sunopsis.sql.DbInt"><![CDATA[80]]></Field>
    <Field name="TaskName1" type="java.lang.String"><![CDATA[Insert parent]]></Field>
    <Field name="TaskType" type="java.lang.String"><![CDATA[J]]></Field>
    <Field name="DefLschemaName" type="java.lang.String"><![CDATA[LGC_STG]]></Field>
    <Field name="DefTxt" type="java.lang.String"><![CDATA[
insert into <?= odiRef.getObjectName("L", "PARENT_TARGET", "LGC_STG", "D") ?>
select *
from <?= odiRef.getObjectName("L", "CHILD_TARGET", "LGC_STG", "D") ?>
    ]]></Field>
  </Object>
</SunopsisExport>`
}

func childScenarioXML() string {
	return `<?xml version="1.0" encoding="ISO-8859-1"?>
<SunopsisExport>
  <Object class="com.sunopsis.dwg.dbobj.SnpScen">
    <Field name="ScenName" type="java.lang.String"><![CDATA[CHILD]]></Field>
    <Field name="ScenNo" type="com.sunopsis.sql.DbInt"><![CDATA[102]]></Field>
    <Field name="ScenVersion" type="java.lang.String"><![CDATA[001]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenStep">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="StepName" type="java.lang.String"><![CDATA[MAP_CHILD]]></Field>
    <Field name="StepType" type="java.lang.String"><![CDATA[M]]></Field>
    <Field name="TableName" type="java.lang.String"><![CDATA[CHILD_TARGET]]></Field>
    <Field name="LschemaName" type="java.lang.String"><![CDATA[LGC_STG]]></Field>
  </Object>
  <Object class="com.sunopsis.dwg.dbobj.SnpScenTask">
    <Field name="Nno" type="com.sunopsis.sql.DbInt"><![CDATA[1]]></Field>
    <Field name="OrdTrt" type="com.sunopsis.sql.DbInt"><![CDATA[80]]></Field>
    <Field name="ScenTaskNo" type="com.sunopsis.sql.DbInt"><![CDATA[80]]></Field>
    <Field name="TaskName1" type="java.lang.String"><![CDATA[Insert child]]></Field>
    <Field name="TaskType" type="java.lang.String"><![CDATA[J]]></Field>
    <Field name="DefLschemaName" type="java.lang.String"><![CDATA[LGC_STG]]></Field>
    <Field name="DefTxt" type="java.lang.String"><![CDATA[
insert into <?= odiRef.getObjectName("L", "CHILD_TARGET", "LGC_STG", "D") ?>
select 1 as id from dual
    ]]></Field>
  </Object>
</SunopsisExport>`
}

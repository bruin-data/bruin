package odi

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"golang.org/x/net/html/charset"
	"gopkg.in/yaml.v3"
)

type ImportOptions struct {
	SourcePath   string
	PipelinePath string
	Connection   string
	Overwrite    bool
}

type ImportResult struct {
	XMLFiles                 int
	Scenarios                int
	SQLAssets                int
	SourceAssets             int
	ControlAssets            int
	SkippedAssets            int
	VariableMacros           int
	ScenarioCallsResolved    int
	VariableMacrosPath       string
	VariableMacrosWritten    bool
	VariableMacrosUpdated    bool
	VariableMacrosSkipped    bool
	ControlFlowWarnings      []ControlFlowWarning
	ControlFlowReportPath    string
	ControlFlowReportWritten bool
	ControlFlowReportSkipped bool
	PipelineCreated          bool
	ImportedAssetPaths       []string
	LogicalSchemaMapping     map[string]string
}

type Project struct {
	Scenarios            []Scenario
	LogicalSchemaMapping map[string]string
	Variables            pipeline.Variables
	VariableMacros       map[string]VariableMacro
	ControlFlowWarnings  []ControlFlowWarning
	XMLFiles             int
}

type Scenario struct {
	Name       string
	Version    string
	Number     string
	SourcePath string
	Steps      []Step
	Tasks      []Task
	Variables  []Variable
}

type Step struct {
	Number       int
	Name         string
	Type         string
	TableName    string
	Lschema      string
	VariableName string
	OkNextStep   string
	KoNextStep   string
	VarOp        string
}

type Task struct {
	StepNumber      int
	TaskNumber      int
	Order           int
	ParentTask      int
	Name1           string
	Name2           string
	Name3           string
	Type            string
	DefText         string
	ColText         string
	DefLschema      string
	ColLschema      string
	DefTechnology   string
	ColTechnology   string
	OriginalCommand string
}

type Variable struct {
	Name     string
	DataType string
	Default  any
	HasValue bool
}

type VariableMacro struct {
	ODIName   string
	BruinName string
	MacroName string
	Body      string
}

type ControlFlowWarning struct {
	Kind           string
	Scenario       string
	ScenarioFile   string
	StepNumber     int
	StepName       string
	StepType       string
	Resolved       bool
	TargetScenario string
	TargetVersion  string
	Message        string
}

type GeneratedAsset struct {
	Asset       *pipeline.Asset
	SourceOnly  bool
	ControlOnly bool
}

type plannedStepAsset struct {
	Scenario  Scenario
	Step      Step
	Tasks     []Task
	TargetRef objectNameRef
	AssetName string
	AssetPath string
}

type plannedScenarioCallAsset struct {
	Scenario  Scenario
	Step      Step
	Task      Task
	Call      scenarioCall
	Target    Scenario
	AssetName string
	AssetPath string
}

type scenarioCall struct {
	Name    string
	Version string
	Command string
}

type generatedAssetNode struct {
	scenarioKey string
	stepNumber  int
	taskOrder   int
	assetName   string
	isCall      bool
	asset       *pipeline.Asset
}

type objectNameRef struct {
	LogicalSchema string
	Schema        string
	Table         string
	AssetName     string
}

type scenarioIndex struct {
	byKey  map[string]Scenario
	byName map[string][]Scenario
}

type xmlExport struct {
	Objects []xmlObject `xml:"Object"`
}

type xmlObject struct {
	Class  string     `xml:"class,attr"`
	Fields []xmlField `xml:"Field"`
}

type xmlField struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

var (
	odiObjectNamePattern  = regexp.MustCompile(`<\?=\s*odiRef\.getObjectName\(\s*"[^"]+"\s*,\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"[^"]+"\s*\)\s*\?>`)
	odiSchemaNamePattern  = regexp.MustCompile(`<\?=\s*odiRef\.getSchemaName\(\s*"([^"]+)"\s*,\s*"[^"]+"\s*\)\s*\?>`)
	odiVariablePattern    = regexp.MustCompile(`#([A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z][A-Za-z0-9_]*)+)`)
	odiMacroPattern       = regexp.MustCompile(`{%-?\s*macro\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
	selectFromDualPattern = regexp.MustCompile(`(?is)^\s*select\s+(.+?)\s+from\s+dual\s*;?\s*$`)
)

func Import(ctx context.Context, fs afero.Fs, opts ImportOptions) (*ImportResult, error) {
	if strings.TrimSpace(opts.SourcePath) == "" {
		return nil, errors.New("source path is required")
	}
	if strings.TrimSpace(opts.PipelinePath) == "" {
		return nil, errors.New("pipeline path is required")
	}

	project, err := ParseProject(fs, opts.SourcePath)
	if err != nil {
		return nil, err
	}

	pipelinePath := resolvePipelinePath(opts.PipelinePath)
	pipelineFile := filepath.Join(pipelinePath, "pipeline.yml")
	if err := fs.MkdirAll(filepath.Join(pipelinePath, "assets"), 0o755); err != nil {
		return nil, errors.Wrap(err, "failed to create pipeline assets directory")
	}

	pipelineCreated, err := ensurePipelineFile(fs, pipelineFile, opts.Connection, project.Variables)
	if err != nil {
		return nil, err
	}

	macrosPath, macrosWritten, macrosUpdated, macrosSkipped, err := ensureVariableMacrosFile(fs, pipelinePath, project.VariableMacros, opts.Overwrite)
	if err != nil {
		return nil, err
	}

	reportPath, reportWritten, reportSkipped, err := ensureControlFlowReportFile(fs, pipelinePath, project.ControlFlowWarnings, opts.Overwrite)
	if err != nil {
		return nil, err
	}

	assets := GenerateAssets(project, filepath.Join(pipelinePath, "assets"), opts.Connection)
	result := &ImportResult{
		XMLFiles:                 project.XMLFiles,
		Scenarios:                len(project.Scenarios),
		VariableMacros:           len(project.VariableMacros),
		VariableMacrosPath:       macrosPath,
		VariableMacrosWritten:    macrosWritten,
		VariableMacrosUpdated:    macrosUpdated,
		VariableMacrosSkipped:    macrosSkipped,
		ControlFlowWarnings:      project.ControlFlowWarnings,
		ControlFlowReportPath:    reportPath,
		ControlFlowReportWritten: reportWritten,
		ControlFlowReportSkipped: reportSkipped,
		PipelineCreated:          pipelineCreated,
		LogicalSchemaMapping:     project.LogicalSchemaMapping,
	}

	for _, generated := range assets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		asset := generated.Asset
		exists, err := afero.Exists(fs, asset.ExecutableFile.Path)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to check if asset file exists: %s", asset.ExecutableFile.Path)
		}
		if exists && !opts.Overwrite {
			result.SkippedAssets++
			continue
		}
		if err := fs.MkdirAll(filepath.Dir(asset.ExecutableFile.Path), 0o755); err != nil {
			return nil, errors.Wrapf(err, "failed to create asset directory: %s", filepath.Dir(asset.ExecutableFile.Path))
		}
		if err := asset.Persist(fs); err != nil {
			return nil, errors.Wrapf(err, "failed to persist ODI asset %q", asset.Name)
		}

		result.ImportedAssetPaths = append(result.ImportedAssetPaths, asset.ExecutableFile.Path)
		switch {
		case generated.SourceOnly:
			result.SourceAssets++
		case generated.ControlOnly:
			result.ControlAssets++
		default:
			result.SQLAssets++
		}
	}
	result.ScenarioCallsResolved = countResolvedScenarioCalls(project.ControlFlowWarnings)

	return result, nil
}

func ParseProject(fs afero.Fs, sourcePath string) (*Project, error) {
	xmlFiles, err := listXMLFiles(fs, sourcePath)
	if err != nil {
		return nil, err
	}
	if len(xmlFiles) == 0 {
		return nil, fmt.Errorf("no ODI XML files found at %q", sourcePath)
	}

	project := &Project{
		LogicalSchemaMapping: make(map[string]string),
		Variables:            pipeline.Variables{},
		XMLFiles:             len(xmlFiles),
	}

	exports := make(map[string]xmlExport, len(xmlFiles))
	for _, filePath := range xmlFiles {
		exp, err := parseXMLFile(fs, filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse ODI XML file %s", filePath)
		}
		exports[filePath] = exp
		mergeLogicalSchemaMapping(project.LogicalSchemaMapping, exp)
	}

	for _, filePath := range xmlFiles {
		scenario, ok := scenarioFromExport(exports[filePath], filePath)
		if !ok {
			continue
		}
		project.Scenarios = append(project.Scenarios, scenario)
		mergeVariables(project.Variables, scenario.Variables)
	}

	sort.Slice(project.Scenarios, func(i, j int) bool {
		return project.Scenarios[i].Name < project.Scenarios[j].Name
	})
	project.VariableMacros = buildVariableMacros(project.Scenarios)
	project.ControlFlowWarnings = detectControlFlowWarnings(project.Scenarios, newScenarioIndex(project.Scenarios))

	return project, nil
}

func GenerateAssets(project *Project, assetsPath, connection string) []GeneratedAsset {
	if project == nil {
		return nil
	}

	targetAssetNames := make(map[string]bool)
	assetNames := make(map[string]bool)
	assetPaths := make(map[string]bool)
	planned := make([]plannedStepAsset, 0)
	referencedSources := make(map[string]objectNameRef)
	scenariosByIdentity := newScenarioIndex(project.Scenarios)

	for _, scenario := range project.Scenarios {
		tasksByStep := groupTasksByStep(scenario.Tasks)
		steps := append([]Step(nil), scenario.Steps...)
		sort.Slice(steps, func(i, j int) bool {
			return steps[i].Number < steps[j].Number
		})

		for _, step := range steps {
			tasks := tasksByStep[step.Number]
			if len(tasks) == 0 {
				continue
			}
			if isVariableStep(step, tasks) {
				continue
			}
			commandTexts := executableTaskTexts(tasks)
			if len(commandTexts) == 0 {
				continue
			}
			if allTasksAreResolvedScenarioCalls(tasks, scenariosByIdentity, scenario) {
				continue
			}

			targetRef := targetRefForStep(step, tasks, project.LogicalSchemaMapping)
			assetName, assetPath := assetNameAndPathForStep(assetsPath, scenario, step, targetRef)
			if assetNames[assetName] || assetPaths[assetPath] {
				assetName, assetPath = fallbackAssetNameAndPathForStep(assetsPath, scenario, step)
				assetName, assetPath = uniqueAssetNameAndPath(assetName, assetPath, assetNames, assetPaths)
			}
			if targetRef.AssetName != "" {
				targetAssetNames[targetRef.AssetName] = true
			}
			assetNames[assetName] = true
			assetPaths[assetPath] = true
			planned = append(planned, plannedStepAsset{
				Scenario:  scenario,
				Step:      step,
				Tasks:     tasks,
				TargetRef: targetRef,
				AssetName: assetName,
				AssetPath: assetPath,
			})
		}
	}

	generated := make([]GeneratedAsset, 0, len(planned))
	nodes := make([]generatedAssetNode, 0, len(planned))
	scenarioAssetNames := make(map[string]map[string]bool)
	previousProducerByObject := make(map[string]string)
	for _, item := range planned {
		renderedSQL, refs := renderTaskSQL(item.Tasks, project.LogicalSchemaMapping)
		upstreams := upstreamsForRefs(refs, item.TargetRef, previousProducerByObject)
		for _, ref := range refs {
			if ref.AssetName == "" || sameObjectRef(ref, item.TargetRef) || targetAssetNames[ref.AssetName] {
				continue
			}
			referencedSources[ref.AssetName] = ref
		}

		asset := &pipeline.Asset{
			Name:        item.AssetName,
			Type:        pipeline.AssetTypeOracleQuery,
			Connection:  connection,
			Description: fmt.Sprintf("Imported from ODI scenario %s step %d (%s)", item.Scenario.Name, item.Step.Number, item.Step.Name),
			Upstreams:   upstreams,
			Meta: pipeline.EmptyStringMap{
				"importer":          "odi",
				"odi_scenario":      item.Scenario.Name,
				"odi_scenario_file": filepath.Base(item.Scenario.SourcePath),
				"odi_step":          item.Step.Name,
				"odi_step_number":   strconv.Itoa(item.Step.Number),
				"odi_step_type":     item.Step.Type,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Name:    filepath.Base(item.AssetPath),
				Path:    item.AssetPath,
				Content: renderedSQL,
			},
		}
		if item.Scenario.Version != "" {
			asset.Meta["odi_scenario_version"] = item.Scenario.Version
		}
		if item.TargetRef.AssetName != "" {
			asset.Meta["odi_target"] = item.TargetRef.AssetName
		}
		generated = append(generated, GeneratedAsset{Asset: asset})
		nodes = append(nodes, generatedAssetNode{
			scenarioKey: scenarioKeyForScenario(item.Scenario),
			stepNumber:  item.Step.Number,
			taskOrder:   firstExecutableTaskOrder(item.Tasks),
			assetName:   asset.Name,
			asset:       asset,
		})
		addScenarioAssetName(scenarioAssetNames, item.Scenario, asset.Name)
		if item.TargetRef.AssetName != "" {
			previousProducerByObject[item.TargetRef.AssetName] = asset.Name
		}
	}

	scenarioCallPlans := planScenarioCallAssets(project, assetsPath, assetNames, assetPaths)
	for _, item := range scenarioCallPlans {
		addScenarioAssetName(scenarioAssetNames, item.Scenario, item.AssetName)
	}
	for _, item := range scenarioCallPlans {
		targetAssets := scenarioAssetNamesForScenario(scenarioAssetNames, item.Target)
		asset := &pipeline.Asset{
			Name:        item.AssetName,
			Type:        pipeline.AssetTypeEmpty,
			Description: fmt.Sprintf("Imported ODI scenario call from %s step %d to %s", item.Scenario.Name, item.Step.Number, item.Call.Name),
			Upstreams:   upstreamsForAssetNames(targetAssets, item.AssetName),
			Meta: pipeline.EmptyStringMap{
				"importer":          "odi",
				"odi_scenario":      item.Scenario.Name,
				"odi_scenario_file": filepath.Base(item.Scenario.SourcePath),
				"odi_step":          item.Step.Name,
				"odi_step_number":   strconv.Itoa(item.Step.Number),
				"odi_step_type":     item.Step.Type,
				"odi_call_scenario": item.Call.Name,
				"odi_command":       singleLine(item.Call.Command),
			},
			ExecutableFile: pipeline.ExecutableFile{
				Name: filepath.Base(item.AssetPath),
				Path: item.AssetPath,
			},
		}
		if item.Scenario.Version != "" {
			asset.Meta["odi_scenario_version"] = item.Scenario.Version
		}
		if item.Call.Version != "" {
			asset.Meta["odi_call_scenario_version"] = item.Call.Version
		}
		generated = append(generated, GeneratedAsset{Asset: asset, ControlOnly: true})
		nodes = append(nodes, generatedAssetNode{
			scenarioKey: scenarioKeyForScenario(item.Scenario),
			stepNumber:  item.Step.Number,
			taskOrder:   taskSortOrder(item.Task),
			assetName:   asset.Name,
			isCall:      true,
			asset:       asset,
		})
	}
	applyScenarioCallOrdering(nodes)

	sourceNames := make([]string, 0, len(referencedSources))
	for name := range referencedSources {
		if targetAssetNames[name] {
			continue
		}
		sourceNames = append(sourceNames, name)
	}
	sort.Strings(sourceNames)

	for _, name := range sourceNames {
		ref := referencedSources[name]
		assetPath := filepath.Join(assetsPath, safePathSegment(ref.Schema), safeFileName(ref.Table)+".asset.yml")
		if assetPaths[assetPath] {
			continue
		}
		asset := &pipeline.Asset{
			Name:        ref.AssetName,
			Type:        pipeline.AssetTypeOracleSource,
			Connection:  connection,
			Description: "ODI referenced Oracle source table " + ref.AssetName,
			Meta: pipeline.EmptyStringMap{
				"importer":           "odi",
				"odi_logical_schema": ref.LogicalSchema,
			},
			ExecutableFile: pipeline.ExecutableFile{
				Name: filepath.Base(assetPath),
				Path: assetPath,
			},
		}
		generated = append(generated, GeneratedAsset{Asset: asset, SourceOnly: true})
	}

	return generated
}

func planScenarioCallAssets(project *Project, assetsPath string, assetNames, assetPaths map[string]bool) []plannedScenarioCallAsset {
	if project == nil {
		return nil
	}

	index := newScenarioIndex(project.Scenarios)
	plans := make([]plannedScenarioCallAsset, 0)
	for _, scenario := range project.Scenarios {
		tasksByStep := groupTasksByStep(scenario.Tasks)
		steps := append([]Step(nil), scenario.Steps...)
		sort.Slice(steps, func(i, j int) bool {
			return steps[i].Number < steps[j].Number
		})

		for _, step := range steps {
			for _, task := range tasksByStep[step.Number] {
				call, ok := scenarioCallFromText(taskCommandText(task))
				if !ok {
					continue
				}

				target, found := index.find(call.Name, call.Version)
				if !found || sameScenarioIdentity(scenario, target) {
					continue
				}

				assetName, assetPath := scenarioCallAssetNameAndPath(assetsPath, scenario, step, task, call)
				if assetNames[assetName] || assetPaths[assetPath] {
					continue
				}
				assetNames[assetName] = true
				assetPaths[assetPath] = true
				plans = append(plans, plannedScenarioCallAsset{
					Scenario:  scenario,
					Step:      step,
					Task:      task,
					Call:      call,
					Target:    target,
					AssetName: assetName,
					AssetPath: assetPath,
				})
			}
		}
	}

	sort.Slice(plans, func(i, j int) bool {
		if plans[i].Scenario.Name != plans[j].Scenario.Name {
			return plans[i].Scenario.Name < plans[j].Scenario.Name
		}
		if plans[i].Step.Number != plans[j].Step.Number {
			return plans[i].Step.Number < plans[j].Step.Number
		}
		if taskSortOrder(plans[i].Task) != taskSortOrder(plans[j].Task) {
			return taskSortOrder(plans[i].Task) < taskSortOrder(plans[j].Task)
		}
		return plans[i].AssetName < plans[j].AssetName
	})

	return plans
}

func scenarioCallAssetNameAndPath(assetsPath string, scenario Scenario, step Step, task Task, call scenarioCall) (string, string) {
	scenarioSegment := safePathSegment(scenario.Name)
	targetSegment := safeFileName(call.Name)
	if targetSegment == "" {
		targetSegment = "scenario"
	}

	stepSegment := fmt.Sprintf("%03d_start_%s", step.Number, targetSegment)
	if call.Version != "" {
		stepSegment += "_v" + safeFileName(call.Version)
	}
	if task.TaskNumber != 0 {
		stepSegment += fmt.Sprintf("_task_%d", task.TaskNumber)
	}

	assetName := strings.Join([]string{"odi", scenarioSegment, stepSegment}, ".")
	return assetName, filepath.Join(assetsPath, "odi", scenarioSegment, stepSegment+".asset.yml")
}

func applyScenarioCallOrdering(nodes []generatedAssetNode) {
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].scenarioKey != nodes[j].scenarioKey {
			return nodes[i].scenarioKey < nodes[j].scenarioKey
		}
		if nodes[i].stepNumber != nodes[j].stepNumber {
			return nodes[i].stepNumber < nodes[j].stepNumber
		}
		if nodes[i].taskOrder != nodes[j].taskOrder {
			return nodes[i].taskOrder < nodes[j].taskOrder
		}
		return nodes[i].assetName < nodes[j].assetName
	})

	lastNodeByScenario := make(map[string]string)
	for _, node := range nodes {
		if node.asset == nil || node.assetName == "" {
			continue
		}

		addUpstreamByName(node.asset, lastNodeByScenario[node.scenarioKey])
		lastNodeByScenario[node.scenarioKey] = node.assetName
	}
}

func addUpstreamByName(asset *pipeline.Asset, upstreamName string) {
	upstreamName = strings.TrimSpace(upstreamName)
	if asset == nil || upstreamName == "" || strings.EqualFold(asset.Name, upstreamName) {
		return
	}
	for _, upstream := range asset.Upstreams {
		if strings.EqualFold(upstream.Value, upstreamName) {
			return
		}
	}
	asset.Upstreams = append(asset.Upstreams, pipeline.Upstream{Type: "asset", Value: upstreamName})
}

func addScenarioAssetName(names map[string]map[string]bool, scenario Scenario, assetName string) {
	assetName = strings.TrimSpace(assetName)
	if assetName == "" {
		return
	}
	key := scenarioKeyForScenario(scenario)
	if names[key] == nil {
		names[key] = make(map[string]bool)
	}
	names[key][assetName] = true
}

func scenarioAssetNamesForScenario(names map[string]map[string]bool, scenario Scenario) []string {
	assetsByName := names[scenarioKeyForScenario(scenario)]
	assetNames := make([]string, 0, len(assetsByName))
	for name := range assetsByName {
		assetNames = append(assetNames, name)
	}
	sort.Strings(assetNames)
	return assetNames
}

func upstreamsForAssetNames(assetNames []string, self string) []pipeline.Upstream {
	upstreams := make([]pipeline.Upstream, 0, len(assetNames))
	seen := make(map[string]bool, len(assetNames))
	for _, name := range assetNames {
		if name == "" || strings.EqualFold(name, self) || seen[strings.ToLower(name)] {
			continue
		}
		seen[strings.ToLower(name)] = true
		upstreams = append(upstreams, pipeline.Upstream{Type: "asset", Value: name})
	}
	sort.Slice(upstreams, func(i, j int) bool {
		return upstreams[i].Value < upstreams[j].Value
	})
	return upstreams
}

func firstExecutableTaskOrder(tasks []Task) int {
	for _, task := range tasks {
		text := taskCommandText(task)
		if strings.TrimSpace(text) == "" || isODIRuntimeCommand(text) {
			continue
		}
		return taskSortOrder(task)
	}
	if len(tasks) == 0 {
		return 0
	}
	return taskSortOrder(tasks[0])
}

func taskSortOrder(task Task) int {
	if task.Order != 0 {
		return task.Order
	}
	if task.TaskNumber != 0 {
		return task.TaskNumber
	}
	return 0
}

func scenarioCallFromText(text string) (scenarioCall, bool) {
	text = strings.TrimSpace(text)
	if !isODIRuntimeCommand(text) {
		return scenarioCall{}, false
	}

	name := odiCommandFlag(text, "SCEN_NAME")
	if name == "" {
		return scenarioCall{}, false
	}

	return scenarioCall{
		Name:    name,
		Version: odiCommandFlag(text, "SCEN_VERSION"),
		Command: text,
	}, true
}

func odiCommandFlag(command, flagName string) string {
	prefix := "-" + strings.ToUpper(flagName) + "="
	for _, field := range strings.Fields(command) {
		if !strings.HasPrefix(strings.ToUpper(field), prefix) {
			continue
		}
		value := strings.TrimSpace(field[len(prefix):])
		value = strings.TrimSuffix(value, ";")
		return strings.Trim(value, `"'`)
	}
	return ""
}

func newScenarioIndex(scenarios []Scenario) scenarioIndex {
	index := scenarioIndex{
		byKey:  make(map[string]Scenario, len(scenarios)),
		byName: make(map[string][]Scenario),
	}
	for _, scenario := range scenarios {
		index.byKey[scenarioKeyForScenario(scenario)] = scenario
		nameKey := scenarioNameKey(scenario.Name)
		index.byName[nameKey] = append(index.byName[nameKey], scenario)
	}
	return index
}

func (index scenarioIndex) find(name, version string) (Scenario, bool) {
	if version != "" {
		scenario, ok := index.byKey[scenarioKey(name, version)]
		return scenario, ok
	}

	matches := index.byName[scenarioNameKey(name)]
	if len(matches) != 1 {
		return Scenario{}, false
	}
	return matches[0], true
}

func scenarioKeyForScenario(scenario Scenario) string {
	return scenarioKey(scenario.Name, scenario.Version)
}

func scenarioKey(name, version string) string {
	return scenarioNameKey(name) + "|" + strings.ToUpper(strings.TrimSpace(version))
}

func scenarioNameKey(name string) string {
	return strings.ToUpper(strings.TrimSpace(name))
}

func sameScenarioIdentity(left, right Scenario) bool {
	return scenarioKeyForScenario(left) == scenarioKeyForScenario(right)
}

func scenarioDisplayName(scenario Scenario) string {
	if strings.TrimSpace(scenario.Version) == "" {
		return scenario.Name
	}
	return scenario.Name + " version " + scenario.Version
}

func countResolvedScenarioCalls(warnings []ControlFlowWarning) int {
	count := 0
	for _, warning := range warnings {
		if warning.Kind == "scenario_call" && warning.Resolved {
			count++
		}
	}
	return count
}

func listXMLFiles(fs afero.Fs, sourcePath string) ([]string, error) {
	info, err := fs.Stat(sourcePath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		if strings.EqualFold(filepath.Ext(sourcePath), ".xml") {
			return []string{sourcePath}, nil
		}
		return nil, fmt.Errorf("ODI source file must have .xml extension: %s", sourcePath)
	}

	var files []string
	if err := afero.Walk(fs, sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".xml") {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func parseXMLFile(fs afero.Fs, filePath string) (xmlExport, error) {
	file, err := fs.Open(filePath)
	if err != nil {
		return xmlExport{}, err
	}
	defer file.Close()

	decoder := xml.NewDecoder(file)
	decoder.CharsetReader = charset.NewReaderLabel

	var exp xmlExport
	if err := decoder.Decode(&exp); err != nil {
		return xmlExport{}, err
	}
	return exp, nil
}

func mergeLogicalSchemaMapping(mapping map[string]string, exp xmlExport) {
	logicalSchema := ""
	for _, obj := range exp.Objects {
		if classHasSuffix(obj.Class, "SnpLschema") {
			logicalSchema = normalizeFieldValue(fieldsByName(obj)["LschemaName"])
			break
		}
	}
	if logicalSchema == "" {
		return
	}

	for _, obj := range exp.Objects {
		if !classHasSuffix(obj.Class, "SnpFKXRef") {
			continue
		}
		fields := fieldsByName(obj)
		if !strings.HasPrefix(normalizeFieldValue(fields["RefKey"]), "SNP_PSCHEMA.") {
			continue
		}
		physical := physicalSchemaFromFQName(normalizeFieldValue(fields["RefObjFQName"]))
		if physical != "" {
			mapping[logicalSchema] = physical
			return
		}
	}
}

func physicalSchemaFromFQName(value string) string {
	if value == "" {
		return ""
	}
	parts := strings.Split(value, ".")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func scenarioFromExport(exp xmlExport, sourcePath string) (Scenario, bool) {
	scenario := Scenario{SourcePath: sourcePath}
	for _, obj := range exp.Objects {
		fields := fieldsByName(obj)
		switch {
		case classHasSuffix(obj.Class, "SnpScen"):
			scenario.Name = normalizeFieldValue(fields["ScenName"])
			scenario.Version = normalizeFieldValue(fields["ScenVersion"])
			scenario.Number = normalizeFieldValue(fields["ScenNo"])
		case classHasSuffix(obj.Class, "SnpScenStep"):
			scenario.Steps = append(scenario.Steps, Step{
				Number:       intField(fields["Nno"]),
				Name:         normalizeFieldValue(fields["StepName"]),
				Type:         normalizeFieldValue(fields["StepType"]),
				TableName:    normalizeFieldValue(fields["TableName"]),
				Lschema:      normalizeFieldValue(fields["LschemaName"]),
				VariableName: normalizeFieldValue(fields["VarName"]),
				OkNextStep:   normalizeFieldValue(fields["OkNextStep"]),
				KoNextStep:   normalizeFieldValue(fields["KoNextStep"]),
				VarOp:        normalizeFieldValue(fields["VarOp"]),
			})
		case classHasSuffix(obj.Class, "SnpScenTask"):
			scenario.Tasks = append(scenario.Tasks, Task{
				StepNumber:    intField(fields["Nno"]),
				TaskNumber:    intField(fields["ScenTaskNo"]),
				Order:         intField(fields["OrdTrt"]),
				ParentTask:    intField(fields["ParScenTaskNo"]),
				Name1:         normalizeFieldValue(fields["TaskName1"]),
				Name2:         normalizeFieldValue(fields["TaskName2"]),
				Name3:         normalizeFieldValue(fields["TaskName3"]),
				Type:          normalizeFieldValue(fields["TaskType"]),
				DefText:       normalizeFieldValue(fields["DefTxt"]),
				ColText:       normalizeFieldValue(fields["ColTxt"]),
				DefLschema:    normalizeFieldValue(fields["DefLschemaName"]),
				ColLschema:    normalizeFieldValue(fields["ColLschemaName"]),
				DefTechnology: normalizeFieldValue(fields["DefTechIntName"]),
				ColTechnology: normalizeFieldValue(fields["ColTechIntName"]),
			})
		case classHasSuffix(obj.Class, "SnpVarScen"):
			if variable := variableFromFields(fields); variable.Name != "" {
				scenario.Variables = append(scenario.Variables, variable)
			}
		}
	}

	if scenario.Name == "" {
		return Scenario{}, false
	}

	sort.Slice(scenario.Steps, func(i, j int) bool {
		return scenario.Steps[i].Number < scenario.Steps[j].Number
	})
	sort.Slice(scenario.Tasks, func(i, j int) bool {
		if scenario.Tasks[i].StepNumber != scenario.Tasks[j].StepNumber {
			return scenario.Tasks[i].StepNumber < scenario.Tasks[j].StepNumber
		}
		if scenario.Tasks[i].Order != scenario.Tasks[j].Order {
			return scenario.Tasks[i].Order < scenario.Tasks[j].Order
		}
		return scenario.Tasks[i].TaskNumber < scenario.Tasks[j].TaskNumber
	})

	return scenario, true
}

func fieldsByName(obj xmlObject) map[string]string {
	fields := make(map[string]string, len(obj.Fields))
	for _, field := range obj.Fields {
		fields[field.Name] = field.Value
	}
	return fields
}

func classHasSuffix(className, suffix string) bool {
	return className == suffix || strings.HasSuffix(className, "."+suffix)
}

func normalizeFieldValue(value string) string {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "null") {
		return ""
	}
	return value
}

func intField(value string) int {
	value = normalizeFieldValue(value)
	if value == "" {
		return 0
	}
	n, _ := strconv.Atoi(value)
	return n
}

func variableFromFields(fields map[string]string) Variable {
	name := normalizeFieldValue(fields["VarName"])
	if name == "" {
		return Variable{}
	}

	dataType := normalizeFieldValue(fields["VarDatatype"])
	for _, fieldName := range []string{"DefN", "DefV", "VarLongValue"} {
		raw := normalizeFieldValue(fields[fieldName])
		if raw == "" {
			continue
		}
		if dataType == "N" || fieldName == "DefN" {
			if n, err := strconv.Atoi(raw); err == nil {
				return Variable{Name: name, DataType: dataType, Default: n, HasValue: true}
			}
		}
		return Variable{Name: name, DataType: dataType, Default: raw, HasValue: true}
	}

	if dataType == "N" {
		return Variable{Name: name, DataType: dataType, Default: 0, HasValue: false}
	}
	return Variable{Name: name, DataType: dataType, Default: "", HasValue: false}
}

func mergeVariables(vars pipeline.Variables, odiVars []Variable) {
	for _, odiVar := range odiVars {
		name := bruinVariableName(odiVar.Name)
		if name == "" {
			continue
		}
		definition := map[string]any{
			"default": odiVar.Default,
		}
		switch odiVar.Default.(type) {
		case int:
			definition["type"] = "integer"
		default:
			definition["type"] = "string"
		}
		if existing, ok := vars[name]; ok {
			if existing["default"] == 0 || existing["default"] == "" {
				vars[name] = definition
			}
			continue
		}
		vars[name] = definition
	}
}

func buildVariableMacros(scenarios []Scenario) map[string]VariableMacro {
	macros := make(map[string]VariableMacro)
	for _, scenario := range scenarios {
		for _, variable := range scenario.Variables {
			macro := newVariableMacro(variable.Name, "")
			if macro.MacroName == "" {
				continue
			}
			if _, ok := macros[macro.MacroName]; !ok {
				macros[macro.MacroName] = macro
			}
		}
	}

	for _, scenario := range scenarios {
		tasksByStep := groupTasksByStep(scenario.Tasks)
		for _, step := range scenario.Steps {
			tasks := tasksByStep[step.Number]
			if !isVariableStep(step, tasks) {
				continue
			}
			body := variableMacroBodyFromTasks(tasks)
			if body == "" {
				continue
			}
			macro := newVariableMacro(step.VariableName, body)
			if macro.MacroName == "" {
				continue
			}
			macros[macro.MacroName] = macro
		}
	}

	return macros
}

func detectControlFlowWarnings(scenarios []Scenario, index scenarioIndex) []ControlFlowWarning {
	var warnings []ControlFlowWarning
	for _, scenario := range scenarios {
		steps := append([]Step(nil), scenario.Steps...)
		sort.Slice(steps, func(i, j int) bool {
			return steps[i].Number < steps[j].Number
		})

		for stepIdx, step := range steps {
			if step.KoNextStep != "" {
				warnings = append(warnings, newControlFlowWarning(scenario, step, "failure_branch", "failure route via KoNextStep="+step.KoNextStep+" requires manual migration review"))
			}
			if step.OkNextStep != "" {
				warnings = append(warnings, successRouteWarnings(scenario, steps, stepIdx)...)
			}
			if step.VarOp != "" && step.VarOp != "=" {
				warnings = append(warnings, newControlFlowWarning(scenario, step, "variable_operation", "unsupported ODI variable operation "+step.VarOp+" requires manual migration review"))
			}
		}

		tasksByStep := groupTasksByStep(scenario.Tasks)
		for _, step := range steps {
			for _, task := range tasksByStep[step.Number] {
				text := taskCommandText(task)
				if !isODIRuntimeCommand(text) {
					continue
				}

				call, ok := scenarioCallFromText(text)
				if !ok {
					warnings = append(warnings, newControlFlowWarning(scenario, step, "scenario_call", "ODI scenario call could not be parsed; preserved as SQL comment: "+singleLine(text)))
					continue
				}

				target, found := index.find(call.Name, call.Version)
				warning := newControlFlowWarning(scenario, step, "scenario_call", "ODI scenario call target was not found in this import; preserved as SQL comment: "+singleLine(text))
				warning.TargetScenario = call.Name
				warning.TargetVersion = call.Version
				if found && !sameScenarioIdentity(scenario, target) {
					warning.Resolved = true
					warning.Message = "ODI scenario call mapped to an empty Bruin asset that depends on imported scenario " + scenarioDisplayName(target)
				} else if found {
					warning.Message = "recursive ODI scenario call requires manual migration review: " + singleLine(text)
				}
				warnings = append(warnings, warning)
			}
		}
	}
	return warnings
}

func successRouteWarnings(scenario Scenario, steps []Step, index int) []ControlFlowWarning {
	step := steps[index]
	okNextStep, err := strconv.Atoi(step.OkNextStep)
	if err != nil {
		return []ControlFlowWarning{
			newControlFlowWarning(scenario, step, "success_route", "could not parse OkNextStep="+step.OkNextStep+"; review ODI success route manually"),
		}
	}

	if okNextStep <= step.Number {
		return []ControlFlowWarning{
			newControlFlowWarning(scenario, step, "loop", fmt.Sprintf("success route jumps backward or to the same step via OkNextStep=%d", okNextStep)),
		}
	}

	if index == len(steps)-1 {
		return []ControlFlowWarning{
			newControlFlowWarning(scenario, step, "success_route", fmt.Sprintf("last step has non-empty OkNextStep=%d; review ODI success route manually", okNextStep)),
		}
	}

	nextStepNumber := steps[index+1].Number
	if okNextStep != nextStepNumber {
		return []ControlFlowWarning{
			newControlFlowWarning(scenario, step, "success_jump", fmt.Sprintf("success route jumps to step %d instead of next linear step %d", okNextStep, nextStepNumber)),
		}
	}

	return nil
}

func newControlFlowWarning(scenario Scenario, step Step, kind, message string) ControlFlowWarning {
	return ControlFlowWarning{
		Kind:         kind,
		Scenario:     scenario.Name,
		ScenarioFile: filepath.Base(scenario.SourcePath),
		StepNumber:   step.Number,
		StepName:     step.Name,
		StepType:     step.Type,
		Message:      message,
	}
}

func singleLine(text string) string {
	fields := strings.Fields(text)
	return strings.Join(fields, " ")
}

func newVariableMacro(odiName, body string) VariableMacro {
	bruinName := bruinVariableName(odiName)
	macroName := odiVariableMacroName(odiName)
	if body == "" && bruinName != "" {
		body = "{{ var." + bruinName + " }}"
	}
	return VariableMacro{
		ODIName:   odiName,
		BruinName: bruinName,
		MacroName: macroName,
		Body:      body,
	}
}

func variableMacroBodyFromTasks(tasks []Task) string {
	for _, task := range tasks {
		text := taskCommandText(task)
		if strings.TrimSpace(text) == "" {
			continue
		}
		if expression := scalarExpressionFromSelectDual(text); expression != "" {
			return renderODIVariableMacroCalls(expression)
		}
		if isSelectStatement(text) {
			return "(" + renderODIVariableMacroCalls(trimStatementTerminator(text)) + ")"
		}
	}
	return ""
}

func scalarExpressionFromSelectDual(sql string) string {
	sql = strings.TrimSpace(sql)
	matches := selectFromDualPattern.FindStringSubmatch(sql)
	if len(matches) != 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func isSelectStatement(sql string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT")
}

func trimStatementTerminator(sql string) string {
	sql = strings.TrimSpace(sql)
	return strings.TrimSuffix(sql, ";")
}

func odiVariableMacroName(name string) string {
	bruinName := bruinVariableName(name)
	if bruinName == "" {
		return ""
	}
	return "odi_" + strings.ToLower(bruinName)
}

func renderODIVariableMacroCalls(sql string) string {
	return odiVariablePattern.ReplaceAllStringFunc(sql, func(match string) string {
		macroName := odiVariableMacroName(strings.TrimPrefix(match, "#"))
		if macroName == "" {
			return match
		}
		return "{{ " + macroName + "() }}"
	})
}

func bruinVariableName(name string) string {
	name = strings.TrimSpace(name)
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - ('a' - 'A'))
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			if b.Len() > 0 && !strings.HasSuffix(b.String(), "_") {
				b.WriteRune('_')
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

func groupTasksByStep(tasks []Task) map[int][]Task {
	grouped := make(map[int][]Task)
	for _, task := range tasks {
		grouped[task.StepNumber] = append(grouped[task.StepNumber], task)
	}
	for step := range grouped {
		sort.Slice(grouped[step], func(i, j int) bool {
			if grouped[step][i].Order != grouped[step][j].Order {
				return grouped[step][i].Order < grouped[step][j].Order
			}
			return grouped[step][i].TaskNumber < grouped[step][j].TaskNumber
		})
	}
	return grouped
}

func executableTaskTexts(tasks []Task) []string {
	var texts []string
	for _, task := range tasks {
		text := taskCommandText(task)
		if strings.TrimSpace(text) == "" {
			continue
		}
		texts = append(texts, text)
	}
	return texts
}

func allTasksAreResolvedScenarioCalls(tasks []Task, index scenarioIndex, scenario Scenario) bool {
	hasCall := false
	for _, task := range tasks {
		text := strings.TrimSpace(taskCommandText(task))
		if text == "" {
			continue
		}
		call, ok := scenarioCallFromText(text)
		if !ok {
			return false
		}
		target, found := index.find(call.Name, call.Version)
		if !found || sameScenarioIdentity(scenario, target) {
			return false
		}
		hasCall = true
	}
	return hasCall
}

func taskCommandText(task Task) string {
	if strings.TrimSpace(task.DefText) != "" {
		return task.DefText
	}
	return task.ColText
}

func isVariableStep(step Step, tasks []Task) bool {
	if strings.EqualFold(step.Type, "V") {
		return true
	}

	for _, task := range tasks {
		if !strings.EqualFold(task.Type, "V") {
			return false
		}
	}
	return len(tasks) > 0
}

func targetRefForStep(step Step, tasks []Task, mapping map[string]string) objectNameRef {
	if step.TableName != "" {
		logicalSchema := step.Lschema
		if logicalSchema == "" {
			for _, task := range tasks {
				if task.DefLschema != "" {
					logicalSchema = task.DefLschema
					break
				}
				if task.ColLschema != "" {
					logicalSchema = task.ColLschema
					break
				}
			}
		}
		schema := physicalSchema(logicalSchema, mapping)
		if schema != "" {
			return newObjectNameRef(logicalSchema, schema, step.TableName)
		}
	}

	for _, task := range tasks {
		text := taskCommandText(task)
		for _, ref := range objectRefsFromSQL(text, mapping) {
			if ref.AssetName != "" {
				return ref
			}
		}
	}

	return objectNameRef{}
}

func assetNameAndPathForStep(assetsPath string, scenario Scenario, step Step, targetRef objectNameRef) (string, string) {
	if targetRef.AssetName != "" {
		return targetRef.AssetName, filepath.Join(assetsPath, safePathSegment(targetRef.Schema), safeFileName(targetRef.Table)+".sql")
	}

	return fallbackAssetNameAndPathForStep(assetsPath, scenario, step)
}

func fallbackAssetNameAndPathForStep(assetsPath string, scenario Scenario, step Step) (string, string) {
	scenarioSegment := safePathSegment(scenario.Name)
	stepSegment := fmt.Sprintf("%03d_%s", step.Number, safeFileName(firstNonEmpty(step.Name, "odi_step")))
	assetName := strings.Join([]string{"odi", scenarioSegment, strings.TrimSuffix(stepSegment, ".sql")}, ".")
	return assetName, filepath.Join(assetsPath, "odi", scenarioSegment, stepSegment+".sql")
}

func uniqueAssetNameAndPath(assetName, assetPath string, assetNames, assetPaths map[string]bool) (string, string) {
	if !assetNames[assetName] && !assetPaths[assetPath] {
		return assetName, assetPath
	}

	ext := filepath.Ext(assetPath)
	pathBase := strings.TrimSuffix(assetPath, ext)
	for suffix := 2; ; suffix++ {
		candidateName := fmt.Sprintf("%s_%d", assetName, suffix)
		candidatePath := fmt.Sprintf("%s_%d%s", pathBase, suffix, ext)
		if !assetNames[candidateName] && !assetPaths[candidatePath] {
			return candidateName, candidatePath
		}
	}
}

func renderTaskSQL(tasks []Task, mapping map[string]string) (string, []objectNameRef) {
	refsByName := make(map[string]objectNameRef)
	var out strings.Builder
	for _, task := range tasks {
		text := taskCommandText(task)
		if strings.TrimSpace(text) == "" {
			continue
		}

		out.WriteString("-- ODI task: ")
		out.WriteString(taskDisplayName(task))
		fmt.Fprintf(&out, " (task_no=%d, order=%d, type=%s)", task.TaskNumber, task.Order, task.Type)
		out.WriteString("\n")

		rendered, refs := renderODIExpressions(text, mapping)
		for _, ref := range refs {
			if ref.AssetName != "" {
				refsByName[ref.AssetName] = ref
			}
		}
		if isODIRuntimeCommand(rendered) {
			out.WriteString(commentLines(rendered))
			out.WriteString("\n\n")
			continue
		}

		out.WriteString(ensureStatementTerminator(strings.TrimSpace(rendered)))
		out.WriteString("\n\n")
	}

	refNames := make([]string, 0, len(refsByName))
	for name := range refsByName {
		refNames = append(refNames, name)
	}
	sort.Strings(refNames)
	refs := make([]objectNameRef, 0, len(refNames))
	for _, name := range refNames {
		refs = append(refs, refsByName[name])
	}

	return strings.TrimSpace(out.String()) + "\n", refs
}

func renderODIExpressions(sql string, mapping map[string]string) (string, []objectNameRef) {
	refsByName := make(map[string]objectNameRef)

	rendered := odiObjectNamePattern.ReplaceAllStringFunc(sql, func(match string) string {
		parts := odiObjectNamePattern.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		table := parts[1]
		logicalSchema := parts[2]
		schema := physicalSchema(logicalSchema, mapping)
		if schema == "" {
			return match
		}
		ref := newObjectNameRef(logicalSchema, schema, table)
		refsByName[ref.AssetName] = ref
		return quoteOracleIdentifier(schema) + "." + quoteOracleIdentifier(table)
	})

	rendered = odiSchemaNamePattern.ReplaceAllStringFunc(rendered, func(match string) string {
		parts := odiSchemaNamePattern.FindStringSubmatch(match)
		if len(parts) != 2 {
			return match
		}
		schema := physicalSchema(parts[1], mapping)
		if schema == "" {
			return match
		}
		return schema
	})

	rendered = renderODIVariableMacroCalls(rendered)

	refNames := make([]string, 0, len(refsByName))
	for name := range refsByName {
		refNames = append(refNames, name)
	}
	sort.Strings(refNames)
	refs := make([]objectNameRef, 0, len(refNames))
	for _, name := range refNames {
		refs = append(refs, refsByName[name])
	}

	return rendered, refs
}

func objectRefsFromSQL(sql string, mapping map[string]string) []objectNameRef {
	_, refs := renderODIExpressions(sql, mapping)
	return refs
}

func newObjectNameRef(logicalSchema, schema, table string) objectNameRef {
	schema = strings.TrimSpace(schema)
	table = strings.TrimSpace(table)
	assetName := ""
	if schema != "" && table != "" {
		assetName = strings.ToLower(schema) + "." + strings.ToLower(table)
	}
	return objectNameRef{
		LogicalSchema: logicalSchema,
		Schema:        schema,
		Table:         table,
		AssetName:     assetName,
	}
}

func physicalSchema(logicalSchema string, mapping map[string]string) string {
	logicalSchema = strings.TrimSpace(logicalSchema)
	if logicalSchema == "" {
		return ""
	}
	if schema := mapping[logicalSchema]; schema != "" {
		return schema
	}
	upper := strings.ToUpper(logicalSchema)
	if schema := mapping[upper]; schema != "" {
		return schema
	}
	if strings.HasPrefix(upper, "LGC_") {
		return strings.TrimPrefix(upper, "LGC_")
	}
	return logicalSchema
}

func upstreamsForRefs(refs []objectNameRef, target objectNameRef, previousProducerByObject map[string]string) []pipeline.Upstream {
	var upstreams []pipeline.Upstream
	seen := make(map[string]bool)
	for _, ref := range refs {
		if ref.AssetName == "" || sameObjectRef(ref, target) || seen[ref.AssetName] {
			continue
		}
		upstreamName := ref.AssetName
		if producer := previousProducerByObject[ref.AssetName]; producer != "" {
			upstreamName = producer
		}
		if seen[upstreamName] {
			continue
		}
		seen[ref.AssetName] = true
		seen[upstreamName] = true
		upstreams = append(upstreams, pipeline.Upstream{Type: "asset", Value: upstreamName})
	}
	sort.Slice(upstreams, func(i, j int) bool {
		return upstreams[i].Value < upstreams[j].Value
	})
	return upstreams
}

func sameObjectRef(a, b objectNameRef) bool {
	return a.AssetName != "" && strings.EqualFold(a.AssetName, b.AssetName)
}

func quoteOracleIdentifier(value string) string {
	value = strings.ReplaceAll(value, `"`, `""`)
	return `"` + value + `"`
}

func taskDisplayName(task Task) string {
	parts := []string{}
	for _, part := range []string{task.Name1, task.Name2, task.Name3} {
		if strings.TrimSpace(part) != "" {
			parts = append(parts, strings.TrimSpace(part))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("task %d", task.TaskNumber)
	}
	return strings.Join(parts, " / ")
}

func isODIRuntimeCommand(sql string) bool {
	return strings.HasPrefix(strings.TrimSpace(strings.ToUpper(sql)), "ODISTARTSCEN")
}

func commentLines(sql string) string {
	lines := strings.Split(strings.TrimSpace(sql), "\n")
	for i, line := range lines {
		lines[i] = "-- ODI command: " + line
	}
	return strings.Join(lines, "\n")
}

func ensureStatementTerminator(sql string) string {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return trimmed
	}
	if strings.HasSuffix(trimmed, ";") || strings.HasSuffix(trimmed, "/") {
		return trimmed
	}
	return trimmed + ";"
}

func safePathSegment(value string) string {
	value = safeFileName(value)
	if value == "" {
		return "odi"
	}
	return value
}

func safeFileName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastUnderscore = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore && b.Len() > 0 {
				b.WriteRune('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func resolvePipelinePath(pipelinePath string) string {
	base := filepath.Base(pipelinePath)
	if base == "pipeline.yml" || base == "pipeline.yaml" {
		return filepath.Dir(pipelinePath)
	}
	return pipelinePath
}

func ensurePipelineFile(fs afero.Fs, pipelineFile, connection string, variables pipeline.Variables) (bool, error) {
	exists, err := afero.Exists(fs, pipelineFile)
	if err != nil {
		return false, err
	}
	if exists {
		return false, mergePipelineVariables(fs, pipelineFile, variables)
	}

	defaultConnections := pipeline.EmptyStringMap(nil)
	if connection != "" {
		defaultConnections = pipeline.EmptyStringMap{"oracle": connection}
	}
	p := &pipeline.Pipeline{
		Name:               safePathSegment(filepath.Base(filepath.Dir(pipelineFile))),
		DefinitionFile:     pipeline.DefinitionFile{Path: pipelineFile},
		DefaultConnections: defaultConnections,
		Variables:          variables,
	}
	return true, p.Persist(fs)
}

func mergePipelineVariables(fs afero.Fs, pipelineFile string, variables pipeline.Variables) error {
	if len(variables) == 0 {
		return nil
	}

	content, err := afero.ReadFile(fs, pipelineFile)
	if err != nil {
		return errors.Wrapf(err, "failed to read existing pipeline file: %s", pipelineFile)
	}

	var root yaml.Node
	if err := yaml.Unmarshal(content, &root); err != nil {
		return errors.Wrapf(err, "failed to parse existing pipeline file: %s", pipelineFile)
	}

	pipelineNode := yamlDocumentMapping(&root)
	if pipelineNode == nil {
		return errors.Errorf("pipeline file must contain a YAML mapping: %s", pipelineFile)
	}

	variablesNode := yamlMappingValue(pipelineNode, "variables")
	if variablesNode == nil {
		variablesNode = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		yamlAppendMappingValue(pipelineNode, "variables", variablesNode)
	}
	if variablesNode.Kind != yaml.MappingNode {
		return errors.Errorf("pipeline variables must be a YAML mapping: %s", pipelineFile)
	}

	changed := false
	names := make([]string, 0, len(variables))
	for name := range variables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if yamlMappingValue(variablesNode, name) != nil {
			continue
		}
		definitionNode, err := variableDefinitionYAMLNode(variables[name])
		if err != nil {
			return errors.Wrapf(err, "failed to serialize ODI variable definition %s", name)
		}
		yamlAppendMappingValue(variablesNode, name, definitionNode)
		changed = true
	}

	if !changed {
		return nil
	}

	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	if err := encoder.Encode(&root); err != nil {
		_ = encoder.Close()
		return errors.Wrapf(err, "failed to serialize updated pipeline file: %s", pipelineFile)
	}
	if err := encoder.Close(); err != nil {
		return errors.Wrapf(err, "failed to serialize updated pipeline file: %s", pipelineFile)
	}

	return afero.WriteFile(fs, pipelineFile, buf.Bytes(), 0o644)
}

func yamlDocumentMapping(root *yaml.Node) *yaml.Node {
	if root == nil {
		return nil
	}
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		root = root.Content[0]
	}
	if root.Kind != yaml.MappingNode {
		return nil
	}
	return root
}

func yamlMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			return mapping.Content[i+1]
		}
	}
	return nil
}

func yamlAppendMappingValue(mapping *yaml.Node, key string, value *yaml.Node) {
	mapping.Content = append(
		mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		value,
	)
}

func variableDefinitionYAMLNode(definition map[string]any) (*yaml.Node, error) {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	keys := make([]string, 0, len(definition))
	for key := range definition {
		if key != "default" && key != "type" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	keys = append([]string{"default", "type"}, keys...)

	for _, key := range keys {
		value, ok := definition[key]
		if !ok {
			continue
		}
		valueNode, err := yamlNodeForValue(value)
		if err != nil {
			return nil, err
		}
		yamlAppendMappingValue(node, key, valueNode)
	}

	return node, nil
}

func yamlNodeForValue(value any) (*yaml.Node, error) {
	var doc yaml.Node
	content, err := yaml.Marshal(value)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(content, &doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: ""}, nil
	}
	return doc.Content[0], nil
}

func ensureVariableMacrosFile(fs afero.Fs, pipelinePath string, macros map[string]VariableMacro, overwrite bool) (string, bool, bool, bool, error) {
	if len(macros) == 0 {
		return "", false, false, false, nil
	}

	macrosFile := filepath.Join(pipelinePath, "macros", "odi_variables.sql")
	exists, err := afero.Exists(fs, macrosFile)
	if err != nil {
		return "", false, false, false, errors.Wrapf(err, "failed to check if ODI variable macros file exists: %s", macrosFile)
	}
	if exists && !overwrite {
		existingContent, err := afero.ReadFile(fs, macrosFile)
		if err != nil {
			return "", false, false, false, errors.Wrapf(err, "failed to read existing ODI variable macros file: %s", macrosFile)
		}
		missingMacros := missingVariableMacros(macros, string(existingContent))
		if len(missingMacros) == 0 {
			return macrosFile, false, false, true, nil
		}

		content := string(existingContent)
		if !strings.HasSuffix(content, "\n") {
			content += "\n"
		}
		content += "\n" + renderVariableMacros(missingMacros)
		if err := afero.WriteFile(fs, macrosFile, []byte(content), 0o644); err != nil {
			return "", false, false, false, errors.Wrapf(err, "failed to append ODI variable macros file: %s", macrosFile)
		}

		return macrosFile, true, true, false, nil
	}

	if err := fs.MkdirAll(filepath.Dir(macrosFile), 0o755); err != nil {
		return "", false, false, false, errors.Wrapf(err, "failed to create ODI variable macros directory: %s", filepath.Dir(macrosFile))
	}
	if err := afero.WriteFile(fs, macrosFile, []byte(renderVariableMacros(macros)), 0o644); err != nil {
		return "", false, false, false, errors.Wrapf(err, "failed to write ODI variable macros file: %s", macrosFile)
	}

	return macrosFile, true, exists, false, nil
}

func missingVariableMacros(macros map[string]VariableMacro, existingContent string) map[string]VariableMacro {
	existing := make(map[string]bool)
	for _, match := range odiMacroPattern.FindAllStringSubmatch(existingContent, -1) {
		if len(match) == 2 {
			existing[match[1]] = true
		}
	}

	missing := make(map[string]VariableMacro)
	for name, macro := range macros {
		if existing[name] {
			continue
		}
		missing[name] = macro
	}
	return missing
}

func ensureControlFlowReportFile(fs afero.Fs, pipelinePath string, warnings []ControlFlowWarning, overwrite bool) (string, bool, bool, error) {
	if len(warnings) == 0 {
		return "", false, false, nil
	}

	reportFile := filepath.Join(pipelinePath, "odi_control_flow_report.yml")
	exists, err := afero.Exists(fs, reportFile)
	if err != nil {
		return "", false, false, errors.Wrapf(err, "failed to check if ODI control-flow report exists: %s", reportFile)
	}
	if exists && !overwrite {
		return reportFile, false, true, nil
	}

	if err := fs.MkdirAll(filepath.Dir(reportFile), 0o755); err != nil {
		return "", false, false, errors.Wrapf(err, "failed to create ODI control-flow report directory: %s", filepath.Dir(reportFile))
	}
	if err := afero.WriteFile(fs, reportFile, []byte(renderControlFlowReport(warnings)), 0o644); err != nil {
		return "", false, false, errors.Wrapf(err, "failed to write ODI control-flow report: %s", reportFile)
	}

	return reportFile, true, false, nil
}

func renderVariableMacros(macros map[string]VariableMacro) string {
	macroNames := make([]string, 0, len(macros))
	for name := range macros {
		macroNames = append(macroNames, name)
	}
	sort.Strings(macroNames)

	var b strings.Builder
	b.WriteString("-- Generated by bruin import odi. Review ODI variable expressions before production use.\n\n")
	for _, name := range macroNames {
		macro := macros[name]
		if macro.Body == "" {
			continue
		}
		fmt.Fprintf(&b, "{%% macro %s() -%%}\n%s\n{%%- endmacro %%}\n\n", macro.MacroName, macro.Body)
	}

	return b.String()
}

func renderControlFlowReport(warnings []ControlFlowWarning) string {
	var b strings.Builder
	b.WriteString("generated_by: \"bruin import odi\"\n")
	b.WriteString("summary: \"ODI control-flow constructs were flattened during import; review before running migrated assets.\"\n")
	b.WriteString("warnings:\n")
	for _, warning := range warnings {
		b.WriteString("  - kind: ")
		b.WriteString(strconv.Quote(warning.Kind))
		b.WriteString("\n")
		b.WriteString("    scenario: ")
		b.WriteString(strconv.Quote(warning.Scenario))
		b.WriteString("\n")
		b.WriteString("    scenario_file: ")
		b.WriteString(strconv.Quote(warning.ScenarioFile))
		b.WriteString("\n")
		b.WriteString("    step_number: ")
		b.WriteString(strconv.Itoa(warning.StepNumber))
		b.WriteString("\n")
		b.WriteString("    step_name: ")
		b.WriteString(strconv.Quote(warning.StepName))
		b.WriteString("\n")
		b.WriteString("    step_type: ")
		b.WriteString(strconv.Quote(warning.StepType))
		b.WriteString("\n")
		if warning.Kind == "scenario_call" {
			b.WriteString("    resolved: ")
			b.WriteString(strconv.FormatBool(warning.Resolved))
			b.WriteString("\n")
			if warning.TargetScenario != "" {
				b.WriteString("    target_scenario: ")
				b.WriteString(strconv.Quote(warning.TargetScenario))
				b.WriteString("\n")
			}
			if warning.TargetVersion != "" {
				b.WriteString("    target_version: ")
				b.WriteString(strconv.Quote(warning.TargetVersion))
				b.WriteString("\n")
			}
		}
		b.WriteString("    message: ")
		b.WriteString(strconv.Quote(warning.Message))
		b.WriteString("\n")
	}
	return b.String()
}

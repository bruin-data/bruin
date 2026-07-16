package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// VariantSet maps a variant name to the variable overrides for that variant.
// The inner map keys must reference variables declared in Pipeline.Variables.
//
//nolint:recvcheck // UnmarshalJSON requires pointer receiver; reads use value receiver.
type VariantSet map[string]map[string]any

var variantNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Names returns the variant names in deterministic (sorted) order.
func (vs VariantSet) Names() []string {
	if len(vs) == 0 {
		return nil
	}
	names := make([]string, 0, len(vs))
	for name := range vs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Validate ensures the variant set is well-formed against the given Variables block.
func (vs VariantSet) Validate(vars Variables) error {
	for name, values := range vs {
		if !variantNameRegex.MatchString(name) {
			return fmt.Errorf("invalid variant name %q: must match [a-zA-Z0-9_-]+", name)
		}
		for key, value := range values {
			schema, ok := vars[key]
			if !ok {
				return fmt.Errorf("variant %q references unknown variable %q", name, key)
			}
			if err := validateOverrideType(value, schema); err != nil {
				return fmt.Errorf("variant %q variable %q: %w", name, key, err)
			}
		}
	}
	return nil
}

func validateOverrideType(value any, schema map[string]any) error {
	declared, ok := schema["type"].(string)
	if !ok || declared == "" {
		return nil
	}
	if matchesDeclaredType(value, declared) {
		return nil
	}
	return fmt.Errorf("type mismatch: expected %s, got %T (%v)", declared, value, value)
}

func matchesDeclaredType(value any, declared string) bool {
	switch declared {
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		switch v := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return true
		case float64:
			return v == float64(int64(v))
		case float32:
			return float32(int64(v)) == v
		default:
			return false
		}
	case "number":
		switch value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
			float32, float64:
			return true
		default:
			return false
		}
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "null":
		return value == nil
	default:
		return true // unknown type — be permissive
	}
}

// UnmarshalJSON allows an empty `{}` to clear the variant set, mirroring Variables.
func (vs *VariantSet) UnmarshalJSON(data []byte) error {
	*vs = make(VariantSet)
	if len(data) == 0 || string(data) == "{}" {
		return nil
	}
	var temp map[string]map[string]any
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	*vs = VariantSet(temp)
	return nil
}

// RenderFunc is the minimal render contract MaterializeVariant needs. It is satisfied
// by jinja.Renderer.Render — wired up in the cmd layer to avoid a pkg/pipeline ↔ pkg/jinja
// import cycle.
type RenderFunc func(string) (string, error)

// VariantRendererFactory builds a RenderFunc for a given (vars, variantName)
// pair. It is passed into NewBuilder so the Builder can materialize variants
// without taking a hard dependency on pkg/jinja.
type VariantRendererFactory func(vars map[string]any, variantName string) RenderFunc

// ApplyVariantVariables merges the named variant's overrides into pl.Variables.
// It is intended to run early (e.g. as a pipeline mutator) so that any later
// rendering — including the asset parameter mutator — sees the variant's values.
func (pl *Pipeline) ApplyVariantVariables(variantName string) error {
	if variantName == "" {
		return errors.New("variant name is required")
	}
	if len(pl.Variants) == 0 {
		return fmt.Errorf("pipeline %q has no variants defined", pl.Name)
	}
	overrides, ok := pl.Variants[variantName]
	if !ok {
		return fmt.Errorf("unknown variant %q (available: %s)", variantName, strings.Join(pl.Variants.Names(), ", "))
	}
	if pl.Variables == nil {
		pl.Variables = make(Variables)
	}
	if err := pl.Variables.Merge(overrides); err != nil {
		return fmt.Errorf("invalid variant %q: %w", variantName, err)
	}
	pl.SelectedVariant = variantName
	return nil
}

// RenderTemplatedFields walks build-time templated string fields on the pipeline and its
// assets and renders them using a renderer built from the supplied factory. It does
// not modify pl.Variables — callers must have already applied any variant or --var
// overrides. Asset bodies (ExecutableFile.Content) are NOT rendered here; they keep
// flowing through Jinja at run time. Query-bearing fields such as hooks, custom checks,
// and SQL-like parameters are also left for run-time Jinja.
//
// After rendering, the in-memory tasksByName index is rebuilt so lookups by the
// rendered asset name work.
func (pl *Pipeline) RenderTemplatedFields(makeRenderer func(vars map[string]any) RenderFunc) error {
	render := makeRenderer(pl.Variables.Value())
	if err := renderPipelineStrings(render, pl); err != nil {
		return err
	}
	pl.tasksByName = make(map[string]*Asset)
	for _, asset := range pl.Assets {
		pl.tasksByName[asset.Name] = asset
	}
	return nil
}

// RenderAssetTemplatedFields renders templated string fields on the given asset
// using the provided render func. Asset bodies (ExecutableFile.Content) are NOT
// rendered. Useful for commands like `internal parse-asset` that produce a single
// asset rather than a full pipeline.
func RenderAssetTemplatedFields(a *Asset, render RenderFunc) error {
	return renderAssetStrings(render, a)
}

// MaterializeVariant is the convenience wrapper that applies the variant's
// variables and renders templated fields in one call. Callers that need the
// variant's variables active during pipeline construction should instead call
// ApplyVariantVariables (e.g. via a pipeline mutator) and RenderTemplatedFields
// separately.
func (pl *Pipeline) MaterializeVariant(variantName string, makeRenderer func(vars map[string]any, variant string) RenderFunc) error {
	if err := pl.ApplyVariantVariables(variantName); err != nil {
		return err
	}
	return pl.RenderTemplatedFields(func(vars map[string]any) RenderFunc {
		return makeRenderer(vars, variantName)
	})
}

func maybeRender(render RenderFunc, fieldPath, value string) (string, error) {
	if value == "" {
		return value, nil
	}
	if !strings.Contains(value, "{{") && !strings.Contains(value, "{%") {
		return value, nil
	}
	out, err := render(value)
	if err != nil {
		return value, fmt.Errorf("rendering %s: %w", fieldPath, err)
	}
	return out, nil
}

func renderPipelineStrings(render RenderFunc, pl *Pipeline) error {
	var err error

	if pl.Name, err = maybeRender(render, "pipeline.name", pl.Name); err != nil {
		return err
	}
	if pl.Owner, err = maybeRender(render, "pipeline.owner", pl.Owner); err != nil {
		return err
	}
	scheduleStr, err := maybeRender(render, "pipeline.schedule", string(pl.Schedule))
	if err != nil {
		return err
	}
	pl.Schedule = Schedule(scheduleStr)
	if pl.StartDate, err = maybeRender(render, "pipeline.start_date", pl.StartDate); err != nil {
		return err
	}
	for i, tag := range pl.Tags {
		if pl.Tags[i], err = maybeRender(render, fmt.Sprintf("pipeline.tags[%d]", i), tag); err != nil {
			return err
		}
	}
	for i, dom := range pl.Domains {
		if pl.Domains[i], err = maybeRender(render, fmt.Sprintf("pipeline.domains[%d]", i), dom); err != nil {
			return err
		}
	}
	for k, v := range pl.Meta {
		if pl.Meta[k], err = maybeRender(render, fmt.Sprintf("pipeline.meta[%s]", k), v); err != nil {
			return err
		}
	}
	for k, v := range pl.DefaultConnections {
		if pl.DefaultConnections[k], err = maybeRender(render, fmt.Sprintf("pipeline.default_connections[%s]", k), v); err != nil {
			return err
		}
	}
	if pl.DefaultValues != nil {
		if err := renderDefaultValues(render, pl.DefaultValues); err != nil {
			return err
		}
	}
	for _, asset := range pl.Assets {
		if err := renderAssetStrings(render, asset); err != nil {
			return err
		}
	}
	return nil
}

// renderDefaultValues renders the identity-shaped fields under `default:` in
// pipeline.yml. Parameters and hook queries are NOT rendered here — those
// contain runtime templates (e.g. {{ start_date }}) and are resolved at
// execution time by the existing per-asset renderer.
func renderDefaultValues(render RenderFunc, dv *DefaultValues) error {
	var err error
	if dv.Type, err = maybeRender(render, "default.type", dv.Type); err != nil {
		return err
	}

	asset := assetFromDefaultValues(dv)
	if err := renderAssetStrings(render, asset); err != nil {
		return err
	}
	copyAssetToDefaultValues(dv, asset)

	return nil
}

func assetFromDefaultValues(dv *DefaultValues) *Asset {
	secrets := make([]SecretMapping, 0, len(dv.Secrets))
	for _, secret := range dv.Secrets {
		secrets = append(secrets, SecretMapping(secret))
	}

	return &Asset{
		Type:              AssetType(dv.Type),
		Description:       dv.Description,
		StartDate:         dv.StartDate,
		Connection:        dv.Connection,
		Tags:              dv.Tags,
		Domains:           dv.Domains,
		Meta:              dv.Meta,
		Materialization:   dv.Materialization,
		Upstreams:         dv.Upstreams,
		Image:             dv.Image,
		Instance:          dv.Instance,
		Owner:             dv.Owner,
		Tier:              dv.Tier,
		Parameters:        ParameterMap(dv.Parameters),
		Secrets:           secrets,
		Extends:           dv.Extends,
		Columns:           dv.Columns,
		CustomChecks:      dv.CustomChecks,
		Hooks:             dv.Hooks,
		Metadata:          dv.Metadata,
		Snowflake:         dv.Snowflake,
		Athena:            dv.Athena,
		Doris:             dv.Doris,
		StarRocks:         dv.StarRocks,
		Routing:           dv.Routing,
		IntervalModifiers: dv.IntervalModifiers,
		RerunCooldown:     dv.RerunCooldown,
		Retries:           dv.Retries,
		Timeout:           dv.Timeout,
		RefreshRestricted: dv.RefreshRestricted,
		Notifications:     dv.Notifications,
	}
}

func copyAssetToDefaultValues(dv *DefaultValues, asset *Asset) {
	secrets := make([]secretMapping, 0, len(asset.Secrets))
	for _, secret := range asset.Secrets {
		secrets = append(secrets, secretMapping(secret))
	}

	dv.Type = string(asset.Type)
	dv.Description = asset.Description
	dv.StartDate = asset.StartDate
	dv.Connection = asset.Connection
	dv.Tags = asset.Tags
	dv.Domains = asset.Domains
	dv.Meta = asset.Meta
	dv.Materialization = asset.Materialization
	dv.Upstreams = asset.Upstreams
	dv.Image = asset.Image
	dv.Instance = asset.Instance
	dv.Owner = asset.Owner
	dv.Tier = asset.Tier
	dv.Parameters = map[string]interface{}(asset.Parameters)
	dv.Secrets = secrets
	dv.Extends = asset.Extends
	dv.Columns = asset.Columns
	dv.CustomChecks = asset.CustomChecks
	dv.Hooks = asset.Hooks
	dv.Metadata = asset.Metadata
	dv.Snowflake = asset.Snowflake
	dv.Athena = asset.Athena
	dv.Doris = asset.Doris
	dv.StarRocks = asset.StarRocks
	dv.Routing = asset.Routing
	dv.IntervalModifiers = asset.IntervalModifiers
	dv.RerunCooldown = asset.RerunCooldown
	dv.Retries = asset.Retries
	dv.Timeout = asset.Timeout
	dv.RefreshRestricted = asset.RefreshRestricted
	dv.Notifications = asset.Notifications
}

func renderAssetStrings(render RenderFunc, a *Asset) error {
	var err error
	originalName := a.Name
	if err := renderTemplatedBool(render, fmt.Sprintf("asset[%s].enabled", originalName), a.Enabled); err != nil {
		return err
	}
	if a.Name, err = maybeRender(render, fmt.Sprintf("asset[%s].name", originalName), a.Name); err != nil {
		return err
	}
	if a.URI, err = maybeRender(render, fmt.Sprintf("asset[%s].uri", originalName), a.URI); err != nil {
		return err
	}
	if a.StartDate, err = maybeRender(render, fmt.Sprintf("asset[%s].start_date", originalName), a.StartDate); err != nil {
		return err
	}
	if a.Description, err = maybeRender(render, fmt.Sprintf("asset[%s].description", originalName), a.Description); err != nil {
		return err
	}
	if a.Connection, err = maybeRender(render, fmt.Sprintf("asset[%s].connection", originalName), a.Connection); err != nil {
		return err
	}
	if a.Image, err = maybeRender(render, fmt.Sprintf("asset[%s].image", originalName), a.Image); err != nil {
		return err
	}
	if a.Instance, err = maybeRender(render, fmt.Sprintf("asset[%s].instance", originalName), a.Instance); err != nil {
		return err
	}
	if a.Owner, err = maybeRender(render, fmt.Sprintf("asset[%s].owner", originalName), a.Owner); err != nil {
		return err
	}
	for i, tag := range a.Tags {
		if a.Tags[i], err = maybeRender(render, fmt.Sprintf("asset[%s].tags[%d]", originalName, i), tag); err != nil {
			return err
		}
	}
	for i, dom := range a.Domains {
		if a.Domains[i], err = maybeRender(render, fmt.Sprintf("asset[%s].domains[%d]", originalName, i), dom); err != nil {
			return err
		}
	}
	for k, v := range a.Meta {
		if a.Meta[k], err = maybeRender(render, fmt.Sprintf("asset[%s].meta[%s]", originalName, k), v); err != nil {
			return err
		}
	}
	for k, v := range a.Metadata {
		if a.Metadata[k], err = maybeRender(render, fmt.Sprintf("asset[%s].metadata[%s]", originalName, k), v); err != nil {
			return err
		}
	}
	// Asset.Parameters is intentionally NOT rendered here. Parameter values
	// frequently embed runtime variables (e.g. "{{ start_date }}") which the
	// per-asset renderer resolves at execution time with the full Jinja context.
	for i := range a.Secrets {
		s := &a.Secrets[i]
		if s.SecretKey, err = maybeRender(render, fmt.Sprintf("asset[%s].secrets[%d].key", originalName, i), s.SecretKey); err != nil {
			return err
		}
		if s.InjectedKey, err = maybeRender(render, fmt.Sprintf("asset[%s].secrets[%d].inject_as", originalName, i), s.InjectedKey); err != nil {
			return err
		}
	}
	for i := range a.Extends {
		if a.Extends[i], err = maybeRender(render, fmt.Sprintf("asset[%s].extends[%d]", originalName, i), a.Extends[i]); err != nil {
			return err
		}
	}

	if a.Materialization.PartitionBy, err = maybeRender(render, fmt.Sprintf("asset[%s].materialization.partition_by", originalName), a.Materialization.PartitionBy); err != nil {
		return err
	}
	if a.Materialization.IncrementalKey, err = maybeRender(render, fmt.Sprintf("asset[%s].materialization.incremental_key", originalName), a.Materialization.IncrementalKey); err != nil {
		return err
	}
	if a.Materialization.IncrementalPredicate, err = maybeRender(render, fmt.Sprintf("asset[%s].materialization.incremental_predicate", originalName), a.Materialization.IncrementalPredicate); err != nil {
		return err
	}
	for i, c := range a.Materialization.ClusterBy {
		if a.Materialization.ClusterBy[i], err = maybeRender(render, fmt.Sprintf("asset[%s].materialization.cluster_by[%d]", originalName, i), c); err != nil {
			return err
		}
	}

	if a.Snowflake.Warehouse, err = maybeRender(render, fmt.Sprintf("asset[%s].snowflake.warehouse", originalName), a.Snowflake.Warehouse); err != nil {
		return err
	}
	if a.Athena.Location, err = maybeRender(render, fmt.Sprintf("asset[%s].athena.location", originalName), a.Athena.Location); err != nil {
		return err
	}
	if a.Doris.TableModel, err = maybeRender(render, fmt.Sprintf("asset[%s].doris.table_model", originalName), a.Doris.TableModel); err != nil {
		return err
	}
	for i, column := range a.Doris.DistributedBy {
		if a.Doris.DistributedBy[i], err = maybeRender(render, fmt.Sprintf("asset[%s].doris.distributed_by[%d]", originalName, i), column); err != nil {
			return err
		}
	}
	for key, value := range a.Doris.Properties {
		if a.Doris.Properties[key], err = maybeRender(render, fmt.Sprintf("asset[%s].doris.properties[%s]", originalName, key), value); err != nil {
			return err
		}
	}
	if a.StarRocks.TableModel, err = maybeRender(render, fmt.Sprintf("asset[%s].starrocks.table_model", originalName), a.StarRocks.TableModel); err != nil {
		return err
	}
	for key, value := range a.StarRocks.Properties {
		if a.StarRocks.Properties[key], err = maybeRender(render, fmt.Sprintf("asset[%s].starrocks.properties[%s]", originalName, key), value); err != nil {
			return err
		}
	}
	if err := renderRoutingConfig(render, fmt.Sprintf("asset[%s].routing", originalName), a.Routing); err != nil {
		return err
	}

	for i := range a.Upstreams {
		u := &a.Upstreams[i]
		if u.Value, err = maybeRender(render, fmt.Sprintf("asset[%s].depends[%d].value", originalName, i), u.Value); err != nil {
			return err
		}
		for k, v := range u.Metadata {
			if u.Metadata[k], err = maybeRender(render, fmt.Sprintf("asset[%s].depends[%d].metadata[%s]", originalName, i, k), v); err != nil {
				return err
			}
		}
		for j := range u.Columns {
			if u.Columns[j].Name, err = maybeRender(render, fmt.Sprintf("asset[%s].depends[%d].columns[%d].name", originalName, i, j), u.Columns[j].Name); err != nil {
				return err
			}
			if u.Columns[j].Usage, err = maybeRender(render, fmt.Sprintf("asset[%s].depends[%d].columns[%d].usage", originalName, i, j), u.Columns[j].Usage); err != nil {
				return err
			}
		}
	}

	for i := range a.Columns {
		c := &a.Columns[i]
		if c.Name, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].name", originalName, i), c.Name); err != nil {
			return err
		}
		if c.SourceColumn, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].source_column", originalName, i), c.SourceColumn); err != nil {
			return err
		}
		if c.Type, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].type", originalName, i), c.Type); err != nil {
			return err
		}
		if c.Mask, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].mask", originalName, i), c.Mask); err != nil {
			return err
		}
		if c.Description, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].description", originalName, i), c.Description); err != nil {
			return err
		}
		if c.MergeSQL, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].merge_sql", originalName, i), c.MergeSQL); err != nil {
			return err
		}
		if c.Owner, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].owner", originalName, i), c.Owner); err != nil {
			return err
		}
		if c.Default, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].default", originalName, i), c.Default); err != nil {
			return err
		}
		if c.Collation, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].collation", originalName, i), c.Collation); err != nil {
			return err
		}
		if c.ForeignKey != nil {
			if c.ForeignKey.Table, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].foreign_key.table", originalName, i), c.ForeignKey.Table); err != nil {
				return err
			}
			if c.ForeignKey.Column, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].foreign_key.column", originalName, i), c.ForeignKey.Column); err != nil {
				return err
			}
		}
		for j, tag := range c.Tags {
			if c.Tags[j], err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].tags[%d]", originalName, i, j), tag); err != nil {
				return err
			}
		}
		for j, dom := range c.Domains {
			if c.Domains[j], err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].domains[%d]", originalName, i, j), dom); err != nil {
				return err
			}
		}
		for k, v := range c.Meta {
			if c.Meta[k], err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].meta[%s]", originalName, i, k), v); err != nil {
				return err
			}
		}
		for j := range c.Checks {
			cc := &c.Checks[j]
			if cc.Name, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].checks[%d].name", originalName, i, j), cc.Name); err != nil {
				return err
			}
			if cc.Description, err = maybeRender(render, fmt.Sprintf("asset[%s].columns[%d].checks[%d].description", originalName, i, j), cc.Description); err != nil {
				return err
			}
		}
	}

	for i := range a.CustomChecks {
		cc := &a.CustomChecks[i]
		if cc.Name, err = maybeRender(render, fmt.Sprintf("asset[%s].custom_checks[%d].name", originalName, i), cc.Name); err != nil {
			return err
		}
		if cc.Description, err = maybeRender(render, fmt.Sprintf("asset[%s].custom_checks[%d].description", originalName, i), cc.Description); err != nil {
			return err
		}
	}

	return nil
}

func renderTemplatedBool(render RenderFunc, fieldPath string, value *TemplatedBool) error {
	if value == nil || value.Template == "" {
		return nil
	}

	rendered, err := maybeRender(render, fieldPath, value.Template)
	if err != nil {
		return err
	}

	parsed, err := strconv.ParseBool(strings.TrimSpace(rendered))
	if err != nil {
		return fmt.Errorf("rendering %s: expected boolean, got %q", fieldPath, rendered)
	}

	value.Value = &parsed
	value.Template = ""
	return nil
}

func renderRoutingConfig(render RenderFunc, path string, routing *RoutingConfig) error {
	if routing == nil {
		return nil
	}

	var err error
	if routing.EgressGateway, err = maybeRender(render, path+".egress_gateway", routing.EgressGateway); err != nil {
		return err
	}

	return nil
}

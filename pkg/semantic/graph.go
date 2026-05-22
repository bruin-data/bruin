package semantic

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type dimensionBinding struct {
	ref          DimensionRef
	model        *Model
	relationName string
	dimension    *Dimension
	outputAlias  string
}

type joinEdge struct {
	from *Model
	to   *Model
	join Join
}

type joinSpec struct {
	model              *Model
	alias              string
	previousModel      *Model
	previousModelAlias string
	join               Join
}

type reachableRelation struct {
	model        *Model
	relationName string
	path         []joinEdge
}

type queryPlan struct {
	engine          *Engine
	joinsByName     map[string]*joinSpec
	joinOrder       []*joinSpec
	dimensionsByKey map[string]*dimensionBinding
	sortsByName     map[string]*dimensionBinding
}

func (e *Engine) planQuery(q *Query) (*queryPlan, error) {
	plan := &queryPlan{
		engine:          e,
		joinsByName:     make(map[string]*joinSpec),
		dimensionsByKey: make(map[string]*dimensionBinding),
		sortsByName:     make(map[string]*dimensionBinding),
	}

	for _, d := range q.Dimensions {
		binding, err := e.resolveDimension(d)
		if err != nil {
			return nil, err
		}
		if err := plan.addDimension(binding); err != nil {
			return nil, err
		}
	}

	for _, f := range q.Filters {
		if f.Expression != "" {
			if err := plan.addFilterRefs(f.Expression); err != nil {
				return nil, err
			}
			continue
		}
		if f.Dimension == "" {
			return nil, errors.New("filter dimension is required")
		}
		binding, err := e.resolveDimension(DimensionRef{Name: f.Dimension})
		if err != nil {
			return nil, fmt.Errorf("filter %w", err)
		}
		if err := plan.addDimension(binding); err != nil {
			return nil, err
		}
	}

	for _, name := range q.Segments {
		segment := e.segments[name]
		if segment == nil {
			continue
		}
		if err := plan.addFilterRefs(segment.Filter); err != nil {
			return nil, err
		}
	}

	for _, sort := range q.Sort {
		if e.metrics[sort.Name] != nil {
			continue
		}
		binding, err := e.resolveDimension(DimensionRef{Name: sort.Name})
		if err != nil {
			continue
		}
		if err := plan.addDimension(binding); err != nil {
			return nil, err
		}
		plan.sortsByName[sort.Name] = binding
	}

	return plan, nil
}

func (p *queryPlan) addFilterRefs(expr string) error {
	for _, ref := range extractRefs(expr) {
		if p.engine.metrics[ref] != nil {
			continue
		}
		binding, err := p.engine.resolveDimension(DimensionRef{Name: ref})
		if err != nil {
			return err
		}
		if err := p.addDimension(binding); err != nil {
			return err
		}
	}
	return nil
}

func (p *queryPlan) addDimension(binding *dimensionBinding) error {
	p.dimensionsByKey[dimensionKey(binding.ref)] = binding
	if binding.model == p.engine.model {
		return nil
	}

	relation, err := p.engine.reachableRelation(binding.relationName, binding.model)
	if err != nil {
		return err
	}
	for _, edge := range relation.path {
		p.ensureJoin(edge)
	}
	return nil
}

func (p *queryPlan) ensureJoin(edge joinEdge) *joinSpec {
	joinName := joinName(edge.join)
	if existing := p.joinsByName[joinName]; existing != nil {
		return existing
	}

	spec := &joinSpec{
		model:              edge.to,
		alias:              sanitizeIdentifier(joinName),
		previousModel:      edge.from,
		previousModelAlias: p.aliasForModel(edge.from),
		join:               edge.join,
	}
	p.joinsByName[joinName] = spec
	p.joinOrder = append(p.joinOrder, spec)
	return spec
}

func (p *queryPlan) aliasForModel(model *Model) string {
	if model == p.engine.model {
		return "base"
	}
	for _, spec := range p.joinOrder {
		if spec.model == model {
			return spec.alias
		}
	}
	return sanitizeIdentifier(model.Name)
}

func (p *queryPlan) dimensionBinding(ref DimensionRef) *dimensionBinding {
	return p.dimensionsByKey[dimensionKey(ref)]
}

func (p *queryPlan) sortBinding(name string) *dimensionBinding {
	return p.sortsByName[name]
}

func (p *queryPlan) hasJoins() bool {
	return len(p.joinOrder) > 0
}

func (p *queryPlan) dimensionSQL(binding *dimensionBinding) string {
	expr := p.engine.dimExpr(binding.dimension, binding.ref.Granularity)
	if binding.model == p.engine.model {
		if p.hasJoins() {
			return qualifySQLIdentifiers(expr, "base")
		}
		return expr
	}
	spec := p.joinsByName[binding.relationName]
	return qualifySQLIdentifiers(expr, spec.alias)
}

func (p *queryPlan) fromSQL() string {
	if len(p.joinOrder) == 0 {
		return " FROM " + p.engine.model.Source.Table
	}

	var sql strings.Builder
	sql.WriteString(" FROM (SELECT * FROM ")
	sql.WriteString(p.engine.model.Source.Table)
	sql.WriteString(") base")
	for _, spec := range p.joinOrder {
		sql.WriteString(" LEFT JOIN (SELECT * FROM ")
		sql.WriteString(spec.model.Source.Table)
		sql.WriteString(") ")
		sql.WriteString(spec.alias)
		sql.WriteString(" ON ")
		sql.WriteString(renderJoinCondition(spec))
	}
	return sql.String()
}

func renderJoinCondition(spec *joinSpec) string {
	if strings.TrimSpace(spec.join.SQL) != "" {
		return renderJoinSQL(spec.join.SQL, spec.previousModel, spec.previousModelAlias, spec.model, spec.alias, spec.join)
	}

	foreignKey := strings.TrimSpace(spec.join.ForeignKey)
	targetKey := strings.TrimSpace(spec.join.TargetKey)
	if targetKey == "" {
		targetKey = strings.TrimSpace(spec.model.PrimaryKey)
	}

	left := qualifyExpression(foreignKey, spec.previousModelAlias)
	right := qualifyExpression(targetKey, spec.alias)
	return left + " = " + right
}

func renderJoinSQL(sql string, previous *Model, previousAlias string, target *Model, targetAlias string, join Join) string {
	replacer := strings.NewReplacer(
		"{"+previous.Name+"}", previousAlias,
		"{"+target.Name+"}", targetAlias,
		"{"+joinName(join)+"}", targetAlias,
	)
	return replacer.Replace(sql)
}

func (e *Engine) resolveDimension(ref DimensionRef) (*dimensionBinding, error) {
	if dim := e.dims[ref.Name]; dim != nil {
		return e.bindDimension(ref, e.model, "", dim), nil
	}

	if relationName, dimName, ok := splitQualifiedName(ref.Name); ok {
		relation, err := e.reachableRelation(relationName, nil)
		if err != nil {
			return nil, err
		}
		dim := dimensionByName(relation.model, dimName)
		if dim == nil {
			return nil, fmt.Errorf("dimension not found: %s", ref.Name)
		}
		return e.bindDimension(ref, relation.model, relation.relationName, dim), nil
	}

	var matches []*dimensionBinding
	for _, relation := range e.reachableRelations() {
		dim := dimensionByName(relation.model, ref.Name)
		if dim == nil {
			continue
		}
		matches = append(matches, e.bindDimension(ref, relation.model, relation.relationName, dim))
	}

	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("dimension not found: %s", ref.Name)
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf("ambiguous dimension %q; use join_name.dimension", ref.Name)
	}
}

func (e *Engine) bindDimension(ref DimensionRef, model *Model, relationName string, dim *Dimension) *dimensionBinding {
	outputAlias := ref.Name
	if model != e.model && strings.Contains(ref.Name, ".") {
		outputAlias = sanitizeIdentifier(ref.Name)
	}
	if ref.Granularity != "" && (model != e.model || strings.Contains(ref.Name, ".")) {
		outputAlias += "_" + sanitizeIdentifier(ref.Granularity)
	}

	return &dimensionBinding{
		ref:          ref,
		model:        model,
		relationName: relationName,
		dimension:    dim,
		outputAlias:  outputAlias,
	}
}

func (e *Engine) reachableRelation(name string, target *Model) (*reachableRelation, error) {
	relations := e.reachableRelations()
	if name != "" {
		var modelMatches []int
		for i := range relations {
			relation := relations[i]
			if relation.relationName == name {
				if target == nil || relation.model == target {
					return &relations[i], nil
				}
				continue
			}
			if relation.model.Name == name && (target == nil || relation.model == target) {
				modelMatches = append(modelMatches, i)
			}
		}
		switch len(modelMatches) {
		case 1:
			return &relations[modelMatches[0]], nil
		case 0:
			return nil, fmt.Errorf("join not found or unsafe: %s", name)
		default:
			return nil, fmt.Errorf("ambiguous join %q; use a join name", name)
		}
	}

	if target != nil {
		var matches []int
		for i := range relations {
			if relations[i].model == target {
				matches = append(matches, i)
			}
		}
		switch len(matches) {
		case 1:
			return &relations[matches[0]], nil
		case 0:
			return nil, fmt.Errorf("no safe join path from %s to %s", e.model.Name, target.Name)
		default:
			return nil, fmt.Errorf("ambiguous join path from %s to %s; use a join name", e.model.Name, target.Name)
		}
	}
	return nil, fmt.Errorf("join not found or unsafe: %s", name)
}

func (e *Engine) reachableRelations() []reachableRelation {
	type queueItem struct {
		model *Model
		path  []joinEdge
	}

	var result []reachableRelation
	queue := []queueItem{{model: e.model}}
	visited := map[string]bool{e.model.Name: true}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		for _, edge := range e.safeEdgesFrom(item.model) {
			name := joinName(edge.join)
			key := item.model.Name + "." + name
			if visited[key] {
				continue
			}
			visited[key] = true

			nextPath := append(append([]joinEdge(nil), item.path...), edge)
			result = append(result, reachableRelation{
				model:        edge.to,
				relationName: name,
				path:         nextPath,
			})
			if !visited[edge.to.Name] {
				visited[edge.to.Name] = true
				queue = append(queue, queueItem{model: edge.to, path: nextPath})
			}
		}
	}

	sort.SliceStable(result, func(i, j int) bool {
		return result[i].relationName < result[j].relationName
	})
	return result
}

func (e *Engine) safeEdgesFrom(from *Model) []joinEdge {
	var edges []joinEdge
	for _, join := range from.Joins {
		if !isSafeRelationship(join.Relationship) {
			continue
		}
		target := e.models[joinModelName(join)]
		if target == nil {
			continue
		}
		if strings.TrimSpace(join.SQL) == "" && strings.TrimSpace(join.TargetKey) == "" && strings.TrimSpace(target.PrimaryKey) == "" {
			continue
		}
		edges = append(edges, joinEdge{from: from, to: target, join: join})
	}
	sort.SliceStable(edges, func(i, j int) bool {
		return joinName(edges[i].join) < joinName(edges[j].join)
	})
	return edges
}

func validateJoins(m *Model) error {
	seen := make(map[string]bool, len(m.Joins))
	for _, join := range m.Joins {
		name := joinName(join)
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("model %q: join name is required", m.Name)
		}
		if seen[name] {
			return fmt.Errorf("model %q: duplicate join %q", m.Name, name)
		}
		seen[name] = true
		switch join.Relationship {
		case "one_to_one", "many_to_one", "one_to_many", "many_to_many":
		default:
			return fmt.Errorf("model %q: join %q has invalid relationship %q", m.Name, name, join.Relationship)
		}
		if strings.TrimSpace(join.SQL) == "" && strings.TrimSpace(join.ForeignKey) == "" {
			return fmt.Errorf("model %q: join %q requires foreign_key or sql", m.Name, name)
		}
		if strings.TrimSpace(join.SQL) == "" && strings.TrimSpace(join.TargetKey) == "" && strings.TrimSpace(join.ForeignKey) != "" {
			// The target model primary_key is validated when all semantic models
			// are available to the engine; a model can still load standalone.
			continue
		}
	}
	return nil
}

func validateJoinTargets(models map[string]*Model) error {
	for _, model := range models {
		for _, join := range model.Joins {
			if !isSafeRelationship(join.Relationship) ||
				strings.TrimSpace(join.SQL) != "" ||
				strings.TrimSpace(join.TargetKey) != "" ||
				strings.TrimSpace(join.ForeignKey) == "" {
				continue
			}

			target := models[joinModelName(join)]
			if target == nil || strings.TrimSpace(target.PrimaryKey) != "" {
				continue
			}
			return fmt.Errorf(
				"model %q: join %q requires target_key or primary_key on target model %q",
				model.Name,
				joinName(join),
				target.Name,
			)
		}
	}
	return nil
}

func isSafeRelationship(relationship string) bool {
	switch relationship {
	case "one_to_one", "many_to_one":
		return true
	default:
		return false
	}
}

func joinName(join Join) string {
	return strings.TrimSpace(join.Name)
}

func joinModelName(join Join) string {
	if strings.TrimSpace(join.Model) != "" {
		return strings.TrimSpace(join.Model)
	}
	return joinName(join)
}

func dimensionByName(m *Model, name string) *Dimension {
	for i := range m.Dimensions {
		if m.Dimensions[i].Name == name {
			return &m.Dimensions[i]
		}
	}
	return nil
}

func splitQualifiedName(name string) (string, string, bool) {
	before, after, ok := strings.Cut(name, ".")
	if !ok || before == "" || after == "" {
		return "", "", false
	}
	return before, after, true
}

func dimensionKey(ref DimensionRef) string {
	return ref.Name + "\x00" + ref.Granularity
}

var (
	identifierSanitizer     = regexp.MustCompile(`[^A-Za-z0-9_]`)
	simpleIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

func sanitizeIdentifier(value string) string {
	value = identifierSanitizer.ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return "field"
	}
	if value[0] >= '0' && value[0] <= '9' {
		return "field_" + value
	}
	return value
}

func qualifyExpression(expr, alias string) string {
	expr = strings.TrimSpace(expr)
	if simpleIdentifierPattern.MatchString(expr) {
		return alias + "." + expr
	}
	return expr
}

var sqlIdentifierKeywords = map[string]bool{
	"ALL": true, "AND": true, "AS": true, "ASC": true, "BETWEEN": true, "BY": true,
	"CASE": true, "CAST": true, "CURRENT": true, "DATE": true, "DATETIME": true,
	"DESC": true, "DISTINCT": true, "ELSE": true, "END": true, "FALSE": true,
	"FILTER": true, "FOLLOWING": true, "FROM": true, "GROUP": true, "HAVING": true,
	"ILIKE": true, "IN": true, "INNER": true, "INTERVAL": true, "IS": true,
	"JOIN": true, "LEFT": true, "LIKE": true, "LIMIT": true, "NOT": true,
	"NULL": true, "ON": true, "OR": true, "ORDER": true, "OUTER": true,
	"OVER": true, "PARTITION": true, "PRECEDING": true, "RANGE": true,
	"RIGHT": true, "ROW": true, "ROWS": true, "SELECT": true, "THEN": true,
	"TIME": true, "TIMESTAMP": true, "TRUE": true, "UNBOUNDED": true,
	"WHEN": true, "WHERE": true, "WITH": true,
	"BIGINT": true, "BOOLEAN": true, "DOUBLE": true, "FLOAT": true, "INTEGER": true,
	"NUMERIC": true, "REAL": true, "STRING": true, "TEXT": true, "VARCHAR": true,
}

func qualifySQLIdentifiers(expr, alias string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" || alias == "" {
		return expr
	}
	if simpleIdentifierPattern.MatchString(expr) {
		return alias + "." + expr
	}

	var out strings.Builder
	for i := 0; i < len(expr); {
		ch := expr[i]
		if ch == '\'' || ch == '"' || ch == '`' {
			end := scanQuoted(expr, i, ch)
			out.WriteString(expr[i:end])
			i = end
			continue
		}
		if isIdentifierStart(ch) {
			end := i + 1
			for end < len(expr) && isIdentifierPart(expr[end]) {
				end++
			}
			ident := expr[i:end]
			if shouldQualifyIdentifier(expr, i, end, ident) {
				out.WriteString(alias)
				out.WriteByte('.')
			}
			out.WriteString(ident)
			i = end
			continue
		}
		out.WriteByte(ch)
		i++
	}
	return out.String()
}

func scanQuoted(expr string, start int, quote byte) int {
	for i := start + 1; i < len(expr); i++ {
		if expr[i] != quote {
			continue
		}
		if quote == '\'' && i+1 < len(expr) && expr[i+1] == '\'' {
			i++
			continue
		}
		return i + 1
	}
	return len(expr)
}

func shouldQualifyIdentifier(expr string, start, end int, ident string) bool {
	if sqlIdentifierKeywords[strings.ToUpper(ident)] {
		return false
	}
	prev := previousNonSpace(expr, start)
	if prev == '.' {
		return false
	}
	next := nextNonSpace(expr, end)
	if next == '(' || next == '.' {
		return false
	}
	return true
}

func previousNonSpace(expr string, start int) byte {
	for i := start - 1; i >= 0; i-- {
		if expr[i] != ' ' && expr[i] != '\t' && expr[i] != '\n' && expr[i] != '\r' {
			return expr[i]
		}
	}
	return 0
}

func nextNonSpace(expr string, start int) byte {
	for i := start; i < len(expr); i++ {
		if expr[i] != ' ' && expr[i] != '\t' && expr[i] != '\n' && expr[i] != '\r' {
			return expr[i]
		}
	}
	return 0
}

func isIdentifierStart(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || (ch >= '0' && ch <= '9')
}

package pipeline

import (
	"errors"
	"fmt"
	stdpath "path"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

type assetSet map[*Asset]struct{}

type selectorGraph struct {
	At              bool
	UpstreamDepth   int
	DownstreamDepth int
}

const selectorAssetDependencyType = "asset"

type assetSelectorResolver struct {
	pipeline    *Pipeline
	pipelineDir string
	upstream    map[*Asset][]*Asset
	downstream  map[*Asset][]*Asset
}

func ResolveSelectorAssets(selector string, p *Pipeline) ([]*Asset, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil, errors.New("selector cannot be empty")
	}

	resolver := newAssetSelectorResolver(p)
	resolved := make(assetSet)

	for _, unionTerm := range strings.Fields(selector) {
		current, err := resolver.resolveUnionTerm(unionTerm)
		if err != nil {
			return nil, err
		}
		mergeAssetSets(resolved, current)
	}

	assets := resolver.orderedAssets(resolved)
	if len(assets) == 0 {
		return nil, fmt.Errorf("selector %q matched no assets", selector)
	}

	return assets, nil
}

func newAssetSelectorResolver(p *Pipeline) *assetSelectorResolver {
	resolver := &assetSelectorResolver{
		pipeline:   p,
		upstream:   make(map[*Asset][]*Asset, len(p.Assets)),
		downstream: make(map[*Asset][]*Asset, len(p.Assets)),
	}
	assetsByName := make(map[string]*Asset, len(p.Assets))

	for _, asset := range p.Assets {
		assetsByName[asset.Name] = asset
	}

	if p.DefinitionFile.Path != "" {
		resolver.pipelineDir = filepath.Dir(p.DefinitionFile.Path)
	}

	for _, asset := range p.Assets {
		for _, upstream := range asset.Upstreams {
			if upstream.Type != "" && upstream.Type != selectorAssetDependencyType {
				continue
			}

			parent := assetsByName[upstream.Value]
			if parent == nil {
				continue
			}

			resolver.link(parent, asset)
		}
	}

	return resolver
}

func (r *assetSelectorResolver) link(parent, child *Asset) {
	if !containsAsset(r.upstream[child], parent) {
		r.upstream[child] = append(r.upstream[child], parent)
	}
	if !containsAsset(r.downstream[parent], child) {
		r.downstream[parent] = append(r.downstream[parent], child)
	}
}

func (r *assetSelectorResolver) resolveUnionTerm(term string) (assetSet, error) {
	parts := strings.Split(term, ",")
	var current assetSet

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("invalid selector %q", term)
		}

		resolved, err := r.resolveSingleSelector(part)
		if err != nil {
			return nil, err
		}

		if current == nil {
			current = resolved
			continue
		}

		current = intersectAssetSets(current, resolved)
	}

	if current == nil {
		current = make(assetSet)
	}

	return current, nil
}

func (r *assetSelectorResolver) resolveSingleSelector(raw string) (assetSet, error) {
	graph, core, err := parseSelectorGraph(raw)
	if err != nil {
		return nil, err
	}

	base, err := r.matchSelectorCore(core)
	if err != nil {
		return nil, err
	}

	if graph.At {
		return r.expandAt(base), nil
	}

	resolved := cloneAssetSet(base)
	if graph.UpstreamDepth != 0 {
		mergeAssetSets(resolved, r.expand(base, r.upstream, graph.UpstreamDepth))
	}
	if graph.DownstreamDepth != 0 {
		mergeAssetSets(resolved, r.expand(base, r.downstream, graph.DownstreamDepth))
	}

	return resolved, nil
}

func parseSelectorGraph(raw string) (selectorGraph, string, error) {
	selector := strings.TrimSpace(raw)
	graph := selectorGraph{}

	if strings.HasPrefix(selector, "@") {
		graph.At = true
		selector = selector[1:]
	}

	var err error
	graph.UpstreamDepth, selector, err = parsePrefixDepth(selector)
	if err != nil {
		return selectorGraph{}, "", err
	}

	graph.DownstreamDepth, selector, err = parseSuffixDepth(selector)
	if err != nil {
		return selectorGraph{}, "", err
	}

	selector = strings.TrimSpace(selector)
	if selector == "" {
		return selectorGraph{}, "", fmt.Errorf("invalid selector %q", raw)
	}

	if graph.At && (graph.UpstreamDepth != 0 || graph.DownstreamDepth != 0) {
		return selectorGraph{}, "", fmt.Errorf("invalid selector %q: cannot combine @ with + graph operators", raw)
	}

	return graph, selector, nil
}

func parsePrefixDepth(selector string) (int, string, error) {
	if selector == "" {
		return 0, selector, nil
	}

	if selector[0] == '+' {
		return -1, selector[1:], nil
	}

	index := 0
	for index < len(selector) && unicode.IsDigit(rune(selector[index])) {
		index++
	}

	if index == 0 || index >= len(selector) || selector[index] != '+' {
		return 0, selector, nil
	}

	depth, err := strconv.Atoi(selector[:index])
	if err != nil || depth <= 0 {
		return 0, "", fmt.Errorf("invalid selector %q", selector)
	}

	return depth, selector[index+1:], nil
}

func parseSuffixDepth(selector string) (int, string, error) {
	if selector == "" {
		return 0, selector, nil
	}

	if strings.HasSuffix(selector, "+") {
		return -1, selector[:len(selector)-1], nil
	}

	index := len(selector) - 1
	for index >= 0 && unicode.IsDigit(rune(selector[index])) {
		index--
	}

	if index < 0 || index == len(selector)-1 || selector[index] != '+' {
		return 0, selector, nil
	}

	depth, err := strconv.Atoi(selector[index+1:])
	if err != nil || depth <= 0 {
		return 0, "", fmt.Errorf("invalid selector %q", selector)
	}

	return depth, selector[:index], nil
}

func (r *assetSelectorResolver) matchSelectorCore(core string) (assetSet, error) {
	method := ""
	value := core

	if idx := strings.Index(core, ":"); idx > 0 {
		candidateMethod := core[:idx]
		if isSupportedSelectorMethod(candidateMethod) {
			method = candidateMethod
			value = core[idx+1:]
		}
	}

	if value == "" {
		return nil, fmt.Errorf("invalid selector %q", core)
	}

	resolved := make(assetSet)
	for _, asset := range r.pipeline.Assets {
		matched, err := r.assetMatchesSelector(asset, method, value)
		if err != nil {
			return nil, err
		}
		if matched {
			resolved[asset] = struct{}{}
		}
	}

	return resolved, nil
}

func isSupportedSelectorMethod(method string) bool {
	switch method {
	case "tag", "path", "file", "fqn":
		return true
	default:
		return false
	}
}

func (r *assetSelectorResolver) assetMatchesSelector(asset *Asset, method, value string) (bool, error) {
	switch method {
	case "":
		return r.matchDefaultSelector(asset, value), nil
	case "tag":
		for _, tag := range asset.Tags {
			if matchScalarSelector(value, tag) {
				return true, nil
			}
		}
		return false, nil
	case "path":
		for _, candidate := range r.assetPathCandidates(asset) {
			if matchPathSelector(value, candidate) {
				return true, nil
			}
		}
		return false, nil
	case "file":
		for _, candidate := range r.assetFileCandidates(asset) {
			if matchScalarSelector(value, candidate) {
				return true, nil
			}
		}
		return false, nil
	case "fqn":
		for _, candidate := range r.assetFQNCandidates(asset) {
			if matchScalarSelector(value, candidate) {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported selector method %q", method)
	}
}

func (r *assetSelectorResolver) matchDefaultSelector(asset *Asset, value string) bool {
	if matchScalarSelector(value, asset.Name) {
		return true
	}

	for _, candidate := range r.assetFileCandidates(asset) {
		if matchScalarSelector(value, candidate) {
			return true
		}
	}

	for _, candidate := range r.assetPathCandidates(asset) {
		if matchPathSelector(value, candidate) {
			return true
		}
	}

	for _, candidate := range r.assetFQNCandidates(asset) {
		if matchScalarSelector(value, candidate) {
			return true
		}
	}

	return false
}

func (r *assetSelectorResolver) assetPathCandidates(asset *Asset) []string {
	candidates := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)

	for _, rawPath := range []string{asset.DefinitionFile.Path, asset.ExecutableFile.Path} {
		if rawPath == "" {
			continue
		}

		for _, candidate := range normalizeSelectorPaths(rawPath, r.pipelineDir) {
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			candidates = append(candidates, candidate)
		}
	}

	return candidates
}

func (r *assetSelectorResolver) assetFileCandidates(asset *Asset) []string {
	candidates := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)

	for _, rawPath := range []string{asset.DefinitionFile.Path, asset.ExecutableFile.Path} {
		if rawPath == "" {
			continue
		}

		base := filepath.Base(rawPath)
		for _, candidate := range fileNameCandidates(base) {
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			candidates = append(candidates, candidate)
		}
	}

	return candidates
}

func (r *assetSelectorResolver) assetFQNCandidates(asset *Asset) []string {
	candidates := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)

	addCandidate := func(candidate string) {
		candidate = strings.Trim(candidate, ".")
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		candidates = append(candidates, candidate)
	}

	addCandidate(asset.Name)
	if r.pipeline.Name != "" {
		addCandidate(r.pipeline.Name + "." + asset.Name)
	}

	for _, rawPath := range r.assetPathCandidates(asset) {
		dotted := trimAllExtensionsFromPath(rawPath)
		dotted = strings.ReplaceAll(dotted, "/", ".")
		addCandidate(dotted)
		if r.pipeline.Name != "" {
			addCandidate(r.pipeline.Name + "." + dotted)
		}
	}

	return candidates
}

func normalizeSelectorPaths(rawPath, pipelineDir string) []string {
	rawPath = filepath.Clean(rawPath)

	candidates := []string{filepath.ToSlash(rawPath)}
	if pipelineDir != "" {
		if relPath, err := filepath.Rel(pipelineDir, rawPath); err == nil {
			candidates = append(candidates, filepath.ToSlash(filepath.Clean(relPath)))
		}
	}

	normalized := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimPrefix(candidate, "./")
		candidate = strings.TrimPrefix(candidate, "/")
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		normalized = append(normalized, candidate)
	}

	return normalized
}

func fileNameCandidates(base string) []string {
	candidates := []string{base}
	stem := trimAllExtensions(base)
	if stem != "" && stem != base {
		candidates = append(candidates, stem)
	}
	return candidates
}

func trimAllExtensionsFromPath(rawPath string) string {
	dir := stdpath.Dir(rawPath)
	base := trimAllExtensions(stdpath.Base(rawPath))
	if dir == "." || dir == "/" {
		return base
	}
	return stdpath.Join(dir, base)
}

func trimAllExtensions(name string) string {
	for {
		ext := filepath.Ext(name)
		if ext == "" {
			return name
		}
		name = strings.TrimSuffix(name, ext)
	}
}

func matchScalarSelector(pattern, candidate string) bool {
	if !hasSelectorWildcard(pattern) {
		return pattern == candidate
	}

	matched, err := stdpath.Match(pattern, candidate)
	return err == nil && matched
}

func matchPathSelector(pattern, candidate string) bool {
	pattern = filepath.ToSlash(pattern)
	pattern = strings.TrimPrefix(pattern, "./")
	pattern = strings.TrimPrefix(pattern, "/")

	if !hasSelectorWildcard(pattern) {
		pattern = strings.TrimSuffix(pattern, "/")
		return candidate == pattern || strings.HasPrefix(candidate, pattern+"/")
	}

	matched, err := stdpath.Match(pattern, candidate)
	if err == nil && matched {
		return true
	}

	for _, prefix := range pathSelectorPrefixes(candidate) {
		matched, err = stdpath.Match(pattern, prefix)
		if err == nil && matched {
			return true
		}
	}

	return false
}

func hasSelectorWildcard(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[")
}

func pathSelectorPrefixes(candidate string) []string {
	prefixes := make([]string, 0, strings.Count(candidate, "/"))

	for prefix := stdpath.Dir(candidate); prefix != "." && prefix != "/" && prefix != candidate; prefix = stdpath.Dir(prefix) {
		prefixes = append(prefixes, prefix)
	}

	return prefixes
}

func (r *assetSelectorResolver) expand(base assetSet, graph map[*Asset][]*Asset, depth int) assetSet {
	expanded := make(assetSet)
	queue := make([]selectorQueueItem, 0, len(base))
	visited := make(map[*Asset]int, len(base))

	for asset := range base {
		queue = append(queue, selectorQueueItem{Asset: asset, Depth: 0})
		visited[asset] = 0
	}

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		if depth >= 0 && item.Depth >= depth {
			continue
		}

		for _, next := range graph[item.Asset] {
			nextDepth := item.Depth + 1
			if seenDepth, seen := visited[next]; seen && seenDepth <= nextDepth {
				continue
			}

			visited[next] = nextDepth
			expanded[next] = struct{}{}
			queue = append(queue, selectorQueueItem{Asset: next, Depth: nextDepth})
		}
	}

	return expanded
}

func (r *assetSelectorResolver) expandAt(base assetSet) assetSet {
	resolved := cloneAssetSet(base)
	descendants := r.expand(base, r.downstream, -1)
	mergeAssetSets(resolved, descendants)
	mergeAssetSets(resolved, r.expand(base, r.upstream, -1))
	mergeAssetSets(resolved, r.expand(descendants, r.upstream, -1))
	return resolved
}

func (r *assetSelectorResolver) orderedAssets(set assetSet) []*Asset {
	ordered := make([]*Asset, 0, len(set))
	for _, asset := range r.pipeline.Assets {
		if _, ok := set[asset]; !ok {
			continue
		}
		ordered = append(ordered, asset)
	}
	return ordered
}

type selectorQueueItem struct {
	Asset *Asset
	Depth int
}

func mergeAssetSets(dst, src assetSet) {
	for asset := range src {
		dst[asset] = struct{}{}
	}
}

func intersectAssetSets(left, right assetSet) assetSet {
	intersection := make(assetSet)
	for asset := range left {
		if _, ok := right[asset]; ok {
			intersection[asset] = struct{}{}
		}
	}
	return intersection
}

func cloneAssetSet(src assetSet) assetSet {
	dst := make(assetSet, len(src))
	for asset := range src {
		dst[asset] = struct{}{}
	}
	return dst
}

func containsAsset(assets []*Asset, target *Asset) bool {
	for _, asset := range assets {
		if asset == target {
			return true
		}
	}
	return false
}

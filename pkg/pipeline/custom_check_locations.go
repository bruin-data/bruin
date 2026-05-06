package pipeline

import "gopkg.in/yaml.v3"

// annotateCustomCheckLocations sets SourceLocation and QueryLocation on each custom check
// by navigating the parsed yaml.Node tree.
//
// lineOffset is the number of file lines that precede the first line of the YAML block.
// For a pure YAML asset file this is 0, so yaml.Node.Line maps directly to the file line.
// For an embedded /* @bruin … @bruin */ block, lineOffset is the 1-based line number of
// the opening marker so that yaml.Node.Line 1 maps to file line (lineOffset + 1).
func annotateCustomCheckLocations(asset *Asset, root *yaml.Node, filePath string, lineOffset int) {
	if len(asset.CustomChecks) == 0 {
		return
	}

	// Navigate: Document → Mapping
	doc := root
	if doc.Kind == yaml.DocumentNode {
		if len(doc.Content) == 0 {
			return
		}
		doc = doc.Content[0]
	}
	if doc.Kind != yaml.MappingNode {
		return
	}

	// Find the "custom_checks" key in the top-level mapping.
	var checksSeqNode *yaml.Node
	for i := 0; i+1 < len(doc.Content); i += 2 {
		if doc.Content[i].Value == "custom_checks" {
			checksSeqNode = doc.Content[i+1]
			break
		}
	}
	if checksSeqNode == nil || checksSeqNode.Kind != yaml.SequenceNode {
		return
	}

	for i, checkNode := range checksSeqNode.Content {
		if i >= len(asset.CustomChecks) {
			break
		}
		if checkNode.Kind != yaml.MappingNode {
			continue
		}

		// Set SourceLocation from the check item node position.
		asset.CustomChecks[i].SourceLocation = &SourceLocation{
			File:   filePath,
			Line:   lineOffset + checkNode.Line,
			Column: checkNode.Column,
		}

		// Find the "query" key inside the check's mapping.
		for j := 0; j+1 < len(checkNode.Content); j += 2 {
			if checkNode.Content[j].Value == "query" {
				qk := checkNode.Content[j] // use the key node for the column reference
				asset.CustomChecks[i].QueryLocation = &SourceLocation{
					File:   filePath,
					Line:   lineOffset + qk.Line,
					Column: qk.Column,
				}
				break
			}
		}
	}
}

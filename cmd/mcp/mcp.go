package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/rudderlabs/analytics-go/v4"
	"github.com/urfave/cli/v3"
)

type JSONRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type JSONRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      interface{}   `json:"id"`
	Result  interface{}   `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
}

type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// This will be a long-running process that communicates with Cursor IDE via stdin/stdout.
// using the Model Context Protocol (JSON-RPC).
func MCPCmd() *cli.Command {
	return &cli.Command{
		Name:        "mcp",
		Usage:       "Start MCP server for Cursor IDE integration",
		Description: "Runs a Model Context Protocol server to provide Bruin context to Cursor IDE",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Enable debug logging for MCP server",
				Value: false,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			debug := c.Bool("debug")

			if debug {
				fmt.Fprintf(os.Stderr, "Starting Bruin MCP server...\n")
			}
			return runMCPServer(debug)
		},
	}
}

func runMCPServer(debug bool) error {
	scanner := bufio.NewScanner(os.Stdin)

	telemetry.SendEvent("mcp_server_start", analytics.Properties{
		"debug_mode": debug,
	})

	// Main loop: read requests from stdin, process them, write responses to stdout
	for scanner.Scan() {
		request := strings.TrimSpace(scanner.Text())

		if debug {
			fmt.Fprintf(os.Stderr, "Received request: %s\n", request)
		}

		var rpcRequest JSONRPCRequest
		if err := json.Unmarshal([]byte(request), &rpcRequest); err != nil {
			if debug {
				fmt.Fprintf(os.Stderr, "Failed to parse JSON-RPC request: %v\n", err)
			}
			continue
		}

		if debug {
			fmt.Fprintf(os.Stderr, "Processing method: %s\n", rpcRequest.Method)
		}
		response := processRequest(rpcRequest, debug)

		if response.JSONRPC != "" && response.ID != nil {
			responseJSON, err := json.Marshal(response)
			if err != nil {
				if debug {
					fmt.Fprintf(os.Stderr, "Failed to marshal response: %v\n", err)
				}
				continue
			}

			fmt.Println(string(responseJSON))

			if debug {
				fmt.Fprintf(os.Stderr, "Sent response: %s\n", string(responseJSON))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from stdin: %w", err)
	}

	return nil
}

func processRequest(req JSONRPCRequest, debug bool) JSONRPCResponse {
	switch req.Method {
	// this is the mcp handshake
	case "initialize":
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]interface{}{
					"tools": map[string]interface{}{},
				},
				"serverInfo": map[string]interface{}{
					"name":    "bruin",
					"version": "0.1.0",
				},
			},
		}
	case "initialized", "notifications/initialized":
		if req.ID == nil {
			return JSONRPCResponse{}
		}
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  nil,
		}
	case "tools/list":
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"tools": []map[string]interface{}{
					{
						"name":        "bruin_get_overview",
						"description": "Get information about Bruin's features and capabilities",
						"inputSchema": map[string]interface{}{
							"type":       "object",
							"properties": map[string]interface{}{},
						},
					},
					{
						"name":        "bruin_get_docs_tree",
						"description": "Get tree view of documentation files for Bruin, including all the supported platforms, data sources and destinations.",
						"inputSchema": map[string]interface{}{
							"type":       "object",
							"properties": map[string]interface{}{},
						},
					},
					{
						"name":        "bruin_get_doc_content",
						"description": "Get the contents of a specific documentation file from Bruin CLI docs. Files are organized in three directories: MCP (general docs), Ingestion (data sources like shopify, stripe, github), and Platforms (destinations like bigquery, snowflake, postgres). Use the appropriate prefix to specify the directory, e.g., 'Platforms/bigquery' for BigQuery platform docs or 'Ingestion/shopify' for Shopify source docs. If unsure about the exact path, use bruin_get_docs_tree first to see all available files.",
						"inputSchema": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"filename": map[string]interface{}{
									"type":        "string",
									"description": "Path to the markdown file. Use format 'Directory/filename' (e.g., 'Platforms/bigquery', 'Ingestion/stripe'). The .md extension is optional.",
								},
							},
							"required": []string{"filename"},
						},
					},
				},
			},
		}
	case "tools/call":
		return handleToolCall(req, debug)

	default:
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &JSONRPCError{
				Code:    -32601,
				Message: "Method not found: " + req.Method,
			},
		}
	}
}

//nolint:unparam
func handleToolCall(req JSONRPCRequest, debug bool) JSONRPCResponse {
	if debug {
		fmt.Fprintf(os.Stderr, "Handling tool call request\n")
	}

	params, ok := req.Params.(map[string]interface{})
	if !ok {
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &JSONRPCError{
				Code:    -32602,
				Message: "Invalid params",
			},
		}
	}

	toolName, ok := params["name"].(string)
	if !ok {
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &JSONRPCError{
				Code:    -32602,
				Message: "Missing tool name",
			},
		}
	}

	switch toolName {
	case "bruin_get_overview":
		telemetry.SendEvent("mcp_tool_call", analytics.Properties{
			"tool_name": "bruin_get_overview",
		})
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"content": []map[string]interface{}{
					{
						"type": "text",
						"text": getBruinInfo(),
					},
				},
			},
		}
	case "bruin_get_docs_tree":
		telemetry.SendEvent("mcp_tool_call", analytics.Properties{
			"tool_name": "bruin_get_docs_tree",
		})
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"content": []map[string]interface{}{
					{
						"type": "text",
						"text": getTreeList(),
					},
				},
			},
		}
	case "bruin_get_doc_content":
		// Extract filename parameter
		args, ok := params["arguments"].(map[string]interface{})
		if !ok {
			return JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error: &JSONRPCError{
					Code:    -32602,
					Message: "Invalid arguments",
				},
			}
		}

		filename, ok := args["filename"].(string)
		if !ok {
			return JSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error: &JSONRPCError{
					Code:    -32602,
					Message: "Missing or invalid filename parameter",
				},
			}
		}

		telemetry.SendEvent("mcp_tool_call", analytics.Properties{
			"tool_name": "bruin_get_doc_content",
			"filename":  filename,
		})

		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"content": []map[string]interface{}{
					{
						"type": "text",
						"text": getDocContent(filename),
					},
				},
			},
		}

	default:
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &JSONRPCError{
				Code:    -32601,
				Message: "Unknown tool: " + toolName,
			},
		}
	}
}

func getBruinInfo() string {
	content, err := DocsFS.ReadFile("overview.md")
	if err != nil {
		return fmt.Sprintf("Error: Could not read overview.md: %v", err)
	}
	return string(content)
}

func getTreeList() string {
	var result strings.Builder
	result.WriteString("```\n")
	result.WriteString("Bruin Documentation\n")

	dirs := []string{
		"getting-started",
		"commands",
		"quality",
		"secrets",
		"deployment",
		"cicd",
		"cloud",
		"vscode-extension",
		"ingestion",
		"platforms",
	}

	for i, dir := range dirs {
		isLast := i == len(dirs)-1
		prefix := "├── "
		if isLast {
			prefix = "└── "
		}
		result.WriteString(prefix + dir + "\n")

		childPrefix := "│   "
		if isLast {
			childPrefix = "    "
		}
		result.WriteString(buildDocsSubTree(dir, childPrefix))
	}

	result.WriteString("```\n")
	return result.String()
}

func getDocContent(filename string) string {
	// Ensure filename has .md extension
	if !strings.HasSuffix(filename, ".md") {
		filename += ".md"
	}

	validPrefixes := []string{"getting-started/", "commands/", "quality/", "secrets/", "deployment/", "cicd/", "cloud/", "vscode-extension/", "ingestion/", "platforms/"}
	for _, prefix := range validPrefixes {
		if strings.HasPrefix(filename, prefix) {
			content, err := DocsFS.ReadFile(filename)
			if err == nil {
				return string(content)
			}
			return fmt.Sprintf("Error: File '%s' not found in %s documentation", filename, strings.TrimSuffix(prefix, "/"))
		}
	}

	return fmt.Sprintf("Error: File '%s' not found. Valid prefixes are: getting-started/, commands/, quality/, secrets/, deployment/, cicd/, cloud/, vscode-extension/, ingestion/, platforms/. Use bruin_get_docs_tree to see all available files.", filename)
}

func buildDocsSubTree(dir string, parentPrefix string) string {
	var result strings.Builder

	entries, err := fs.ReadDir(DocsFS, dir)
	if err != nil {
		return fmt.Sprintf("Error reading directory %s: %v\n", dir, err)
	}


	var filtered []fs.DirEntry
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".md") {
			filtered = append(filtered, entry)
		}
	}

	sortedEntries := sortEmbeddedEntries(filtered)

	for i, entry := range sortedEntries {
		isLast := i == len(sortedEntries)-1
		prefix := "├── "
		if isLast {
			prefix = "└── "
		}

		result.WriteString(parentPrefix + prefix + entry.Name() + "\n")

		if entry.IsDir() {
			childPrefix := parentPrefix + "│   "
			if isLast {
				childPrefix = parentPrefix + "    "
			}
			subPath := filepath.Join(dir, entry.Name())
			result.WriteString(buildDocsSubTree(subPath, childPrefix))
		}
	}

	return result.String()
}

func sortEmbeddedEntries(entries []fs.DirEntry) []fs.DirEntry {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir() != entries[j].IsDir() {
			return entries[i].IsDir()
		}
		return entries[i].Name() < entries[j].Name()
	})
	return entries
}

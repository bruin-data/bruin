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

	"github.com/bruin-data/bruin/docs/ingestion"
	"github.com/bruin-data/bruin/docs/platforms"
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
		Action: func(_ context.Context, c *cli.Command) error {
			debug := c.Bool("debug")

			if debug {
				fmt.Fprintf(os.Stderr, "Starting Bruin MCP server...\n")
			}
			return runMCPServer(debug) //nolint:contextcheck
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
		// Documentation tools
		tools := []map[string]interface{}{
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
				"description": "Get the contents of a specific documentation from Bruin CLI docs.",
				"inputSchema": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"filename": map[string]interface{}{
							"type":        "string",
							"description": "Name of the markdown file to fetch (with or without .md extension)",
						},
					},
					"required": []string{"filename"},
				},
			},
		}

		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]interface{}{
				"tools": tools,
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
	content, err := DocsFS.ReadFile("docs/overview.md")
	if err != nil {
		return fmt.Sprintf("Error: Could not read overview.md: %v", err)
	}
	return string(content)
}

func getTreeList() string {
	var result strings.Builder
	result.WriteString("Bruin Documentation\n")

	// MCP docs (existing)
	result.WriteString("    MCP\n")
	result.WriteString(buildEmbeddedTree("docs", 1))

	// Ingestion docs
	result.WriteString("    Ingestion\n")
	result.WriteString(buildIngestionTree(1))

	// Platforms docs
	result.WriteString("    Platforms\n")
	result.WriteString(buildPlatformsTree(1))

	return result.String()
}

func formatEntryName(name string) string {
	return name
}

func getDocContent(filename string) string {
	// Ensure filename has .md extension
	if !strings.HasSuffix(filename, ".md") {
		filename += ".md"
	}

	// Try to find in MCP docs first
	filePath, err := findEmbeddedFile("docs", filename)
	if err == nil {
		content, err := DocsFS.ReadFile(filePath)
		if err == nil {
			return string(content)
		}
	}

	// Try to find in ingestion docs
	content, err := ingestion.DocsFS.ReadFile(filename)
	if err == nil {
		return string(content)
	}

	// Try to find in platforms docs
	content, err = platforms.DocsFS.ReadFile(filename)
	if err == nil {
		return string(content)
	}

	return fmt.Sprintf("Error: File '%s' not found in any documentation directory", filename)
}

func buildEmbeddedTree(rootPath string, depth int) string {
	var result strings.Builder

	entries, err := fs.ReadDir(DocsFS, rootPath)
	if err != nil {
		return fmt.Sprintf("Error reading directory %s: %v\n", rootPath, err)
	}

	sortedEntries := sortEmbeddedEntries(entries)

	for _, entry := range sortedEntries {
		indent := strings.Repeat("    ", depth+1)
		name := formatEntryName(entry.Name())

		result.WriteString(indent + name + "\n")

		if entry.IsDir() {
			subPath := filepath.Join(rootPath, entry.Name())
			result.WriteString(buildEmbeddedTree(subPath, depth+1))
		}
	}

	return result.String()
}

func buildIngestionTree(depth int) string {
	var result strings.Builder

	entries, err := fs.ReadDir(ingestion.DocsFS, ".")
	if err != nil {
		return fmt.Sprintf("Error reading ingestion directory: %v\n", err)
	}

	sortedEntries := sortEmbeddedEntries(entries)

	for _, entry := range sortedEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".md") {
			indent := strings.Repeat("    ", depth+1)
			name := formatEntryName(entry.Name())
			result.WriteString(indent + name + "\n")
		}
	}

	return result.String()
}

func buildPlatformsTree(depth int) string {
	var result strings.Builder

	entries, err := fs.ReadDir(platforms.DocsFS, ".")
	if err != nil {
		return fmt.Sprintf("Error reading platforms directory: %v\n", err)
	}

	sortedEntries := sortEmbeddedEntries(entries)

	for _, entry := range sortedEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".md") {
			indent := strings.Repeat("    ", depth+1)
			name := formatEntryName(entry.Name())
			result.WriteString(indent + name + "\n")
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

func findEmbeddedFile(rootPath, filename string) (string, error) {
	var foundPath string

	// Walk through all directories to find the file
	err := fs.WalkDir(DocsFS, rootPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && d.Name() == filename {
			foundPath = path
			return fs.SkipAll // Stop walking after finding the file
		}

		return nil
	})
	if err != nil {
		return "", fmt.Errorf("error searching for file: %w", err)
	}

	if foundPath == "" {
		return "", fmt.Errorf("file '%s' not found in docs directory", filename)
	}

	return foundPath, nil
}

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/docs"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/rudderlabs/analytics-go/v4"
	"github.com/urfave/cli/v3"
	"github.com/xlab/treeprint"
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
	// This is the mcp handshake
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
						"description": "Get the contents of a specific documentation file from Bruin CLI docs. Use bruin_get_docs_tree first to see all available directories and files. You can access files in subdirectories (e.g., 'ingestion/shopify', 'platforms/bigquery', 'commands/run') or root-level files (e.g., 'overview', 'index'). The .md extension is optional.",
						"inputSchema": map[string]interface{}{
							"type": "object",
							"properties": map[string]interface{}{
								"filename": map[string]interface{}{
									"type":        "string",
									"description": "Path to the markdown file (e.g., 'ingestion/shopify', 'platforms/bigquery', 'overview'). The .md extension is optional.",
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
	content, err := docs.DocsFS.ReadFile("overview.md")
	if err != nil {
		return fmt.Sprintf("Error: Could not read overview.md: %v", err)
	}
	return string(content)
}

func getTreeList() string {
	tree := treeprint.NewWithRoot("Bruin Documentation")
	buildDocTree(tree, ".")
	return "```\n" + tree.String() + "```\n"
}

func buildDocTree(branch treeprint.Tree, dir string) {
	entries, err := fs.ReadDir(docs.DocsFS, dir)
	if err != nil {
		return
	}

	var dirs []fs.DirEntry
	var files []fs.DirEntry
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry)
		} else if strings.HasSuffix(entry.Name(), ".md") {
			files = append(files, entry)
		}
	}

	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Name() < dirs[j].Name() })
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })

	for _, entry := range dirs {
		childPath := dir + "/" + entry.Name()
		if dir == "." {
			childPath = entry.Name()
		}
		childBranch := branch.AddBranch(entry.Name())
		buildDocTree(childBranch, childPath)
	}

	for _, entry := range files {
		branch.AddNode(entry.Name())
	}
}

func getDocContent(filename string) string {
	// Ensure filename has .md extension
	if !strings.HasSuffix(filename, ".md") {
		filename += ".md"
	}

	// Try to read the file directly (handles both root-level and nested files)
	content, err := docs.DocsFS.ReadFile(filename)
	if err == nil {
		return string(content)
	}

	entries, err := fs.ReadDir(docs.DocsFS, ".")
	if err != nil {
		return fmt.Sprintf("Error reading docs: %v", err)
	}

	var validDirs []string
	var rootFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			validDirs = append(validDirs, entry.Name()+"/")
		} else if strings.HasSuffix(entry.Name(), ".md") {
			rootFiles = append(rootFiles, entry.Name())
		}
	}

	return fmt.Sprintf("Error: File '%s' not found. Valid paths are: %s or root files like %s. Use bruin_get_docs_tree to see all available files.", filename, strings.Join(validDirs, ", "), strings.Join(rootFiles, ", "))
}

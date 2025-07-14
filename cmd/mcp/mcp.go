package mcp

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
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
		Action: func(cCtx *cli.Context) error {
			debug := cCtx.Bool("debug")

			if debug {
				fmt.Fprintf(os.Stderr, "Starting Bruin MCP server...\n")
			}
			return runMCPServer(debug)
		},
	}
}

func runMCPServer(debug bool) error {
	scanner := bufio.NewScanner(os.Stdin)

	if debug {
		fmt.Fprintf(os.Stderr, "MCP server ready, waiting for requests...\n")
	}

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

		if response.JSONRPC != "" {
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
	case "initialized":
		// this is also part of  the mcp handshake
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
						"name":        "get_bruin_info",
						"description": "Get information about Bruin features and capabilities",
						"inputSchema": map[string]interface{}{
							"type":       "object",
							"properties": map[string]interface{}{},
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
	case "get_bruin_info":
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
	return `# Bruin Data Platform

Bruin is packed with features:

📥 **ingest data** with ingestr / Python
✨ **run SQL & Python transformations** on many platforms
📐 **table/view materializations**, incremental tables
🐍 **run Python** in isolated environments using uv
💅 **built-in data quality checks**
🔗 **visualize dependencies** with lineage
🔍 **compare tables** across connections with data-diff
🧙 **Jinja templating** to avoid repetition
✅ **validate pipelines** end-to-end via dry-run
👷 **run on your local machine**, an EC2 instance, or GitHub Actions
🔒 **secrets injection** via environment variables
📚 **shared terminology** via glossaries
🆚 **VS Code extension** for a better developer experience
⚡ **written in Golang**

For more information, visit: https://bruin-data.github.io/bruin/`
}

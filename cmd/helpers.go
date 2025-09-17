package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/manifoldco/promptui"
	"github.com/urfave/cli/v3"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

type ErrorResponses struct {
	Error []string `json:"error"`
}

type SuccessResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type WarningResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func switchEnvironment(env string, force bool, cm *config.Config, stdin io.ReadCloser) error {
	if env == "" {
		return nil
	}

	err := cm.SelectEnvironment(env)
	if err != nil {
		errorPrinter.Printf("Failed to use the environment '%s': %v\n", env, err)
		return cli.Exit("", 1)
	}

	// if env name is similar to "prod" ask for confirmation
	if !force && strings.Contains(strings.ToLower(env), "prod") {
		prompt := promptui.Prompt{
			Label:     "You are using a production environment. Are you sure you want to continue?",
			IsConfirm: true,
			Stdin:     stdin,
		}

		_, err := prompt.Run()
		if err != nil {
			fmt.Printf("The operation is cancelled.\n")
			return cli.Exit("", 1)
		}
	}

	return nil
}

func RecoverFromPanic() {
	if err := recover(); err != nil {
		log.Println("=======================================")
		log.Println("Bruin encountered an unexpected error, please report the issue to the Bruin team.")
		log.Println(err)
		log.Println("=======================================")
		b := bufio.NewScanner(bytes.NewBuffer(debug.Stack()))
		for b.Scan() {
			log.Println(b.Text())
		}
		os.Exit(1)
	}
}

func marshal[K ErrorResponse | ErrorResponses](m K) ([]byte, error) {
	js, marshalError := json.Marshal(m)
	if marshalError != nil {
		fmt.Println(marshalError)
		return []byte{}, marshalError
	}
	return js, nil
}

func printErrorJSON(err error) {
	errResponse := ErrorResponse{
		Error: errors.New("something went wrong").Error(),
	}
	if err != nil {
		errResponse.Error = err.Error()
	}
	js, err := marshal[ErrorResponse](errResponse)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(js))
}

func printErrors(errs []error, output string, message string) {
	if output == "json" {
		errorList := []string{}
		for _, v := range errs {
			errorList = append(errorList, v.Error())
		}

		js, err := marshal[ErrorResponses](ErrorResponses{
			Error: errorList,
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(string(js))
	} else {
		errorPrinter.Printf("%s: %v\n", message, fmt.Sprint(errs))
	}
}

func printError(err error, output string, message string) {
	if output == "json" {
		printErrorJSON(err)
	} else {
		errorPrinter.Printf("%s: %v\n", message, err)
	}
}

func NewRunID() string {
	runID := time.Now().Format("2006_01_02_15_04_05")
	if os.Getenv("BRUIN_RUN_ID") != "" {
		runID = os.Getenv("BRUIN_RUN_ID")
	}
	return runID
}

func printSuccessForOutput(output string, message string) {
	if output == "json" {
		successResponse := SuccessResponse{
			Status:  "success",
			Message: message,
		}
		jsonData, err := json.Marshal(successResponse)
		if err != nil {
			fmt.Println("Error:", err.Error())
			return
		}
		fmt.Println(string(jsonData))
	} else {
		successPrinter.Printf("%s\n", message)
	}
}

func printWarningForOutput(output string, message string) {
	if output == "json" {
		warningResponse := WarningResponse{
			Status:  "warning",
			Message: message,
		}
		jsonData, err := json.Marshal(warningResponse)
		if err != nil {
			fmt.Println("Error:", err.Error())
			return
		}
		fmt.Println(string(jsonData))
	} else {
		warningPrinter.Printf("%s\n", message)
	}
}

// connectionTypeMap maps package paths to connection type names
var connectionTypeMap = map[string]string{
	"duckdb":     "duckdb",
	"bigquery":   "bigquery",
	"postgres":   "postgres",
	"snowflake":  "snowflake",
	"mysql":      "mysql",
	"mssql":      "mssql",
	"clickhouse": "clickhouse",
	"athena":     "athena",
	"databricks": "databricks",
	"oracle":     "oracle",
	"sqlite":     "sqlite",
	"trino":      "trino",
	"synapse":    "synapse",
	"hana":       "hana",
	"spanner":    "spanner",
}

// getConnectionType determines the connection type from the connection object
func getConnectionType(conn interface{}) string {
	if conn == nil {
		return "unknown"
	}

	// Use reflection to determine the connection type
	connType := reflect.TypeOf(conn)
	if connType == nil {
		return "unknown"
	}

	// If it's a pointer, get the element type
	if connType.Kind() == reflect.Ptr {
		connType = connType.Elem()
	}

	// Get the package name to determine the connection type
	pkgPath := connType.PkgPath()

	// Check each known connection type
	for pkgName, connTypeName := range connectionTypeMap {
		if strings.Contains(pkgPath, pkgName) {
			return connTypeName
		}
	}

	return "unknown"
}

// formatValue properly formats values based on the connection type
// Handles platform-specific value formatting (e.g., DuckDB float tuples, etc.)
func formatValue(val interface{}, connectionType string) string {
	if val == nil {
		return ""
	}

	// Only apply DuckDB-specific formatting for DuckDB connections
	if connectionType != "duckdb" {
		// For non-DuckDB connections, use standard formatting
		switch v := val.(type) {
		case float64:
			return fmt.Sprintf("%g", v)
		case float32:
			return fmt.Sprintf("%g", v)
		case int64:
			return fmt.Sprintf("%d", v)
		case int32:
			return fmt.Sprintf("%d", v)
		case int:
			return fmt.Sprintf("%d", v)
		case string:
			return v
		case bool:
			return fmt.Sprintf("%t", v)
		default:
			return fmt.Sprintf("%v", val)
		}
	}

	// Handle different types that DuckDB might return
	switch v := val.(type) {
	case float64:
		// Format float64 with appropriate precision
		return fmt.Sprintf("%g", v)
	case float32:
		// Format float32 with appropriate precision
		return fmt.Sprintf("%g", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	case string:
		return v
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		// Try to handle DuckDB decimal types using reflection
		// DuckDB might return a struct that implements certain methods
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Struct {
			// Try to access fields by index (DuckDB structs might have fields in order: Width, Scale, Value)
			if rv.NumField() >= 3 {
				widthField := rv.Field(0)
				scaleField := rv.Field(1)
				valueField := rv.Field(2)

				if widthField.IsValid() && scaleField.IsValid() && valueField.IsValid() {
					// This might be a DuckDB decimal struct
					if valueField.CanInterface() && scaleField.CanInterface() {
						value := valueField.Interface()
						scale := scaleField.Interface()

						// Convert value to float64
						var floatValue float64
						switch v := value.(type) {
						case *big.Int:
							// Convert big.Int to float64
							floatValue, _ = new(big.Float).SetInt(v).Float64()
						case int64:
							floatValue = float64(v)
						case int32:
							floatValue = float64(v)
						case int:
							floatValue = float64(v)
						case float64:
							floatValue = v
						case float32:
							floatValue = float64(v)
						default:
							// Fallback to string representation
							return fmt.Sprintf("%v", val)
						}

						// Convert scale to int
						var scaleInt int
						switch s := scale.(type) {
						case uint8:
							scaleInt = int(s)
						case int64:
							scaleInt = int(s)
						case int32:
							scaleInt = int(s)
						case int:
							scaleInt = s
						default:
							// Fallback: assume scale of 1
							scaleInt = 1
						}

						// Convert using the scale: divide by 10^scale
						divisor := 1.0
						for i := 0; i < scaleInt; i++ {
							divisor *= 10
						}
						return fmt.Sprintf("%g", floatValue/divisor)
					}
				}
			}
		}

		// For other types, try to extract the actual value
		valStr := fmt.Sprintf("%v", val)

		// Check if it's a DuckDB tuple-like string (contains braces)
		// DuckDB returns floats as {width scale value} format
		// Examples: {2 1 15} for 1.5, {6 5 314159} for 3.14159
		if strings.HasPrefix(valStr, "{") && strings.HasSuffix(valStr, "}") {
			// Remove braces and split by spaces
			inner := strings.Trim(valStr, "{}")
			parts := strings.Fields(inner)
			if len(parts) >= 3 {
				// For DuckDB float tuples, the format is {width scale value}
				// where the second part is the scale and the last part is the value
				scaleStr := parts[1]
				valueStr := parts[2]

				if scale, err := strconv.Atoi(scaleStr); err == nil {
					if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
						// Convert using the scale: divide by 10^scale
						divisor := 1.0
						for i := 0; i < scale; i++ {
							divisor *= 10
						}
						return fmt.Sprintf("%g", value/divisor)
					}
				}
			}
		}

		// Check if it's a tuple-like string (contains parentheses)
		if strings.Contains(valStr, "(") && strings.Contains(valStr, ")") {
			// Try to extract the numeric value from tuple-like strings
			// This handles cases where DuckDB returns floats as tuples
			if strings.HasPrefix(valStr, "(") && strings.HasSuffix(valStr, ")") {
				// Remove parentheses and try to parse as float
				inner := strings.Trim(valStr, "()")
				if innerFloat, err := strconv.ParseFloat(inner, 64); err == nil {
					return fmt.Sprintf("%g", innerFloat)
				}
			}
		}

		// Handle single-element slices (might be how DuckDB returns single values)
		if rv.Kind() == reflect.Slice && rv.Len() == 1 {
			elem := rv.Index(0)
			if elem.CanInterface() {
				return formatValue(elem.Interface(), connectionType)
			}
		}

		return valStr
	}
}

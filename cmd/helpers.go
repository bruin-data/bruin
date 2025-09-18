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

// Asset type to connection type mapping.
var assetTypeToConnectionMap = map[string]string{
	"duckdb.sql":        "duckdb",
	"duckdb.seed":       "duckdb",
	"duckdb.source":     "duckdb",
	"bq.sql":            "bigquery",
	"bq.seed":           "bigquery",
	"bq.source":         "bigquery",
	"sf.sql":            "snowflake",
	"sf.seed":           "snowflake",
	"sf.source":         "snowflake",
	"pg.sql":            "postgres",
	"pg.seed":           "postgres",
	"pg.source":         "postgres",
	"ms.sql":            "mssql",
	"ms.seed":           "mssql",
	"ms.source":         "mssql",
	"athena.sql":        "athena",
	"athena.seed":       "athena",
	"athena.source":     "athena",
	"clickhouse.sql":    "clickhouse",
	"clickhouse.seed":   "clickhouse",
	"clickhouse.source": "clickhouse",
	"databricks.sql":    "databricks",
	"databricks.seed":   "databricks",
	"databricks.source": "databricks",
	"synapse.sql":       "synapse",
	"synapse.seed":      "synapse",
	"synapse.source":    "synapse",
	"trino.sql":         "trino",
	"oracle.sql":        "oracle",
	"oracle.source":     "oracle",
	"hana.sql":          "hana",
	"hana.source":       "hana",
	"spanner.sql":       "spanner",
	"spanner.source":    "spanner",
}

const unknownConnectionType = "unknown"

var knownConnections = []string{
	"duckdb", "bigquery", "postgres", "snowflake", "mysql", "mssql",
	"clickhouse", "athena", "databricks", "oracle", "sqlite", "trino",
	"synapse", "hana", "spanner", "redshift",
}

func getConnectionTypeFromAssetType(assetType string) string {
	if assetType == "" {
		return unknownConnectionType
	}

	if connType, exists := assetTypeToConnectionMap[assetType]; exists {
		return connType
	}

	return unknownConnectionType
}

func getConnectionType(conn interface{}) string {
	if conn == nil {
		return unknownConnectionType
	}

	connType := reflect.TypeOf(conn)
	if connType == nil {
		return unknownConnectionType
	}

	if connType.Kind() == reflect.Ptr {
		connType = connType.Elem()
	}

	pkgPath := connType.PkgPath()

	for _, connName := range knownConnections {
		if strings.Contains(pkgPath, connName) {
			return connName
		}
	}

	return unknownConnectionType
}

func formatValue(val interface{}, connectionType string) string {
	if val == nil {
		return ""
	}

	if connectionType == "duckdb" {
		return formatValueForDuckDB(val)
	}

	switch v := val.(type) {
	case float64:
		return fmt.Sprintf("%g", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", val)
	}
}


func formatValueForDuckDB(val interface{}) string {
	switch v := val.(type) {
	case float64:
		return fmt.Sprintf("%g", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	default:
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Struct {
			if rv.NumField() >= 3 {
				widthField := rv.Field(0)
				scaleField := rv.Field(1)
				valueField := rv.Field(2)

				if widthField.IsValid() && scaleField.IsValid() && valueField.IsValid() {
					if valueField.CanInterface() && scaleField.CanInterface() {
						value := valueField.Interface()
						scale := scaleField.Interface()

						var floatValue float64
						switch v := value.(type) {
						case *big.Int:
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
							return fmt.Sprintf("%v", val)
						}

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
							scaleInt = 1
						}

						divisor := 1.0
						for range scaleInt {
							divisor *= 10
						}
						return fmt.Sprintf("%g", floatValue/divisor)
					}
				}
			}
		}

		valStr := fmt.Sprintf("%v", val)

		if strings.HasPrefix(valStr, "{") && strings.HasSuffix(valStr, "}") {
			inner := strings.Trim(valStr, "{}")
			parts := strings.Fields(inner)
			if len(parts) >= 3 {
				scaleStr := parts[1]
				valueStr := parts[2]

				if scale, err := strconv.Atoi(scaleStr); err == nil {
					if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
						divisor := 1.0
						for range scale {
							divisor *= 10
						}
						return fmt.Sprintf("%g", value/divisor)
					}
				}
			}
		}

		if strings.Contains(valStr, "(") && strings.Contains(valStr, ")") {
			if strings.HasPrefix(valStr, "(") && strings.HasSuffix(valStr, ")") {
				inner := strings.Trim(valStr, "()")
				if innerFloat, err := strconv.ParseFloat(inner, 64); err == nil {
					return fmt.Sprintf("%g", innerFloat)
				}
			}
		}

		if rv.Kind() == reflect.Slice && rv.Len() == 1 {
			elem := rv.Index(0)
			if elem.CanInterface() {
				return formatValue(elem.Interface(), "duckdb")
			}
		}

		return valStr
	}
}
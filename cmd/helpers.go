package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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

// convertValueToString converts an interface{} value to a string representation,
// properly handling DuckDB decimal types and other special cases.
func convertValueToString(val interface{}) string {
	if val == nil {
		return ""
	}

	// Check if this is a DuckDB decimal by looking at the string representation
	valStr := fmt.Sprintf("%v", val)
	if strings.HasPrefix(valStr, "{") && strings.HasSuffix(valStr, "}") {
		// This looks like a DuckDB decimal string: "{5 2 99999}"
		content := strings.Trim(valStr, "{}")
		parts := strings.Fields(content)
		if len(parts) == 3 {
			// Parse width, scale, value
			if _, err1 := strconv.ParseInt(parts[0], 10, 64); err1 == nil {
				if scale, err2 := strconv.ParseInt(parts[1], 10, 64); err2 == nil {
					if value, err3 := strconv.ParseInt(parts[2], 10, 64); err3 == nil {
						// Convert integer value to decimal representation
						// value = 99999, scale = 2 -> 999.99
						divisor := int64(1)
						for i := int64(0); i < scale; i++ {
							divisor *= 10
						}

						decimalValue := float64(value) / float64(divisor)
						return strconv.FormatFloat(decimalValue, 'f', int(scale), 64)
					}
				}
			}
		}
	}

	// Handle other numeric types
	switch v := val.(type) {
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case bool:
		return strconv.FormatBool(v)
	default:
		// Fallback to fmt.Sprintf for any other types
		return fmt.Sprintf("%v", val)
	}
}

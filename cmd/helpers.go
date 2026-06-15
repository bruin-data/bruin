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
	"strings"
	"sync/atomic"
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

func switchEnvironment(env string, force bool, cm *config.Config, stdin io.ReadCloser, onlyTaskTypes ...[]string) error {
	if env == "" {
		return nil
	}

	err := cm.SelectEnvironment(env)
	if err != nil {
		errorPrinter.Printf("Failed to use the environment '%s': %v\n", env, err)
		return cli.Exit("", 1)
	}

	// skip confirmation when only running checks, since checks don't modify data
	onlyChecks := len(onlyTaskTypes) > 0 && len(onlyTaskTypes[0]) == 1 && onlyTaskTypes[0][0] == "checks"

	// if env name is similar to "prod" ask for confirmation
	if !force && !onlyChecks && strings.Contains(strings.ToLower(env), "prod") {
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
		errorList := make([]string, 0, len(errs))
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

// runIDCounter guarantees uniqueness for run ids generated within the same
// process even when the OS clock resolution is coarser than a nanosecond (e.g.
// macOS reports microsecond-granularity wall times).
var runIDCounter atomic.Uint64

// NewRunID returns a unique identifier for a pipeline run. The id doubles as the
// run-log filename (logs/runs/<pipeline>/<run-id>.json), so it must be unique
// even for runs that start within the same second — otherwise fast back-to-back
// runs (e.g. backfill chunks) would overwrite each other's logs. We append the
// nanosecond component of the current time plus a monotonically increasing
// per-process counter to the second-granularity timestamp. An explicit
// BRUIN_RUN_ID always takes precedence.
func NewRunID() string {
	if envRunID := os.Getenv("BRUIN_RUN_ID"); envRunID != "" {
		return envRunID
	}
	now := time.Now()
	seq := runIDCounter.Add(1)
	return fmt.Sprintf("%s_%09d_%d", now.Format("2006_01_02_15_04_05"), now.Nanosecond(), seq)
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

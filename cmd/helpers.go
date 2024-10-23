package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/manifoldco/promptui"
	"github.com/urfave/cli/v2"
)

func switchEnvironment(c *cli.Context, cm *config.Config, stdin io.ReadCloser) error {
	env := c.String("environment")
	if env == "" {
		return nil
	}

	err := cm.SelectEnvironment(env)
	if err != nil {
		errorPrinter.Printf("Failed to use the environment '%s': %v\n", env, err)
		return cli.Exit("", 1)
	}

	// if env name is similar to "prod" ask for confirmation
	if !c.Bool("force") && strings.Contains(strings.ToLower(env), "prod") {
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
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

type ErrorResponses struct {
	Error []string `json:"error"`
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
	js, err := marshal[ErrorResponse](ErrorResponse{
		Error: err.Error(),
	})
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

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	jd "github.com/josephburnett/jd/lib"
)

func main() {
	current, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	integrationTestsFolder := current + "/integration-tests"

	entries, err := os.ReadDir(integrationTestsFolder)

	if err != nil {
		fmt.Printf("failed to read directory %s: %s\n", integrationTestsFolder, err.Error())
		os.Exit(2)
	}

	// Iterate over entries
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == ".git" || entry.Name() == "logs" {
			continue
		}
		// Check if the entry is a directory
		runTest(integrationTestsFolder+"/"+entry.Name(), integrationTestsFolder)
	}
}

func runTest(folder, integrationTestsFolder string) {
	fmt.Println("Running test for:", folder)
	current, _ := os.Getwd()
	binaryPath, _ := filepath.Abs(filepath.Join(current, "bin", "bruin"))
	cmd := exec.Command(binaryPath, "validate", folder)
	stdout, err := cmd.Output()
	fmt.Println(string(stdout))
	if err != nil {
		fmt.Println(err)
		os.Exit(3)
	}

	cmd = exec.Command(binaryPath, "run", "--use-uv", folder)
	stdout, err = cmd.Output()
	fmt.Println(string(stdout))
	if err != nil {
		fmt.Println(err)
		os.Exit(4)
	}

	cmd = exec.Command(binaryPath, "internal", "parse-pipeline", folder)
	stdout, err = cmd.Output()
	if err != nil {
		fmt.Println("Error running parse-pipeline")
		fmt.Println(err)
		os.Exit(4)
	}
	expectation, err := jd.ReadJsonFile(folder + "/expectations/pipeline.yml.json")
	if err != nil {
		fmt.Println("Error running expectation for parse-pipeline")
		fmt.Println(err)
		os.Exit(5)
	}

	parsed, err := jd.ReadJsonString(strings.ReplaceAll(string(stdout), integrationTestsFolder, "__BASEDIR__"))
	if err != nil {
		fmt.Println("Error parsing json output for pipeline " + folder)
		fmt.Println(err)
		os.Exit(5)
	}

	diff := expectation.Diff(parsed)
	if len(diff) != 0 {
		fmt.Println("Parsed pipeline not matching")
		fmt.Println(diff.Render())
		os.Exit(6)
	}

	assets, err := os.ReadDir(folder + "/assets")
	if err != nil {
		fmt.Println("Error reading assets folder")
		fmt.Println(err)
		os.Exit(5)
	}
	for _, asset := range assets {
		if asset.IsDir() {
			continue
		}
		fmt.Println("Checking expectations for:" + asset.Name())
		cmd = exec.Command(binaryPath, "internal", "parse-asset", folder+"/assets/"+asset.Name()) //nolint:gosec
		stdout, err = cmd.Output()
		if err != nil {
			fmt.Println("Error running parse asset")
			fmt.Println(err)
			os.Exit(7)
		}

		expectation, err = jd.ReadJsonFile(folder + "/expectations/" + asset.Name() + ".json")
		if err != nil {
			fmt.Println("Error reading expectation for parse asset")
			fmt.Println(err)
			os.Exit(8)
		}

		replaced := strings.ReplaceAll(string(stdout), integrationTestsFolder, "__BASEDIR__")
		parsed, err = jd.ReadJsonString(replaced)
		if err != nil {
			fmt.Println("Error parsing json output for asset " + asset.Name())
			fmt.Println(err)
			os.Exit(8)
		}
		diff = expectation.Diff(parsed)
		if len(diff) != 0 {
			fmt.Printf("Asset %s not matching\n", asset.Name())
			fmt.Println(diff.Render())
			os.Exit(6)
		}
	}
}

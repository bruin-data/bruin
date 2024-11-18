package main

import (
	"context"
	"fmt"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/python"
	"os"
	"strings"

)

func main() {
	fmt.Println("running...")

	cmdRunner := &python.CommandRunner{}
	repo := &git.Repo{
		Path: "C:\\Users\\burak\\code\\bruin",
	}

	
	env := os.Environ()
	kv := map[string]string{}
	for _, v := range env {
		fmt.Println("handling:", v)
		fields := strings.Split(v, "=")
		if len(fields)!= 2 {
			fmt.Println("skipped:", v, fields)
			continue
		}
		kv[fields[0]] = fields[1]
	}

	err := cmdRunner.Run(context.Background(), repo, &python.CommandInstance{
		Name: "C:\\Users\\burak\\.local\\bin\\uv.exe",
		Args: []string{"run", "--no-project", "--python", "3.11", "--with", "platformdirs", `C:\Users\burak\code\bruin\testt.py`},
		EnvVars: kv,
	})
	if err != nil {
		fmt.Println("ilki faileddd")
		fmt.Println(err)
	}


}

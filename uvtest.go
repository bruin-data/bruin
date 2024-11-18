package main

import (
	"context"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/python"
)

func main() {
	cmdRunner := &python.CommandRunner{}
	repo, err := git.FindRepoFromPath("/Users/burak/Code/personal/bruin/internal/bruin-cli")
	if err != nil {
		panic(err)
	}

	err = cmdRunner.Run(context.Background(), repo, &python.CommandInstance{
		Name: "/Users/burak/.local/bin/uv",
		Args: []string{"run", "--no-project", "--python", "3.11", "--with", "platformdirs", "/Users/burak/Code/personal/bruin/internal/bruin-cli/testt.py"},
	})
	if err != nil {
		panic(err)
	}
}

package main

import (
	"github.com/kluctl/go-embed-python/pip"
)

func main() {
	err := pip.CreateEmbeddedPipPackagesForKnownPlatforms("requirements.txt", "./data/")
	if err != nil {
		panic(err)
	}
}

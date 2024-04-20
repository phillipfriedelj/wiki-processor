package main

import (
	"github.com/phillipfriedelj/wiki-processor/cmd/internal/cli"
)

func main() {
	command := cli.ParseCommandLineArgs()

	command.Validate()
	command.Run()
}

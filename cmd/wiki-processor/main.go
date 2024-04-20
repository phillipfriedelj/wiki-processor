package main

import (
	"fmt"

	"github.com/phillipfriedelj/wiki-processor/cmd/internal/cli"
)

func main() {
	command := cli.ParseCommandLineArgs()

	err := command.Validate()
	if err != nil {
		fmt.Println("Error in request: ", err)
	}
	err = command.Run()
	if err != nil {
		fmt.Println("Error running action: ", err)
	}
}

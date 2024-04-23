package main

import (
	"fmt"
	"time"

	"github.com/phillipfriedelj/wiki-processor/cmd/internal/cli"
)

func main() {
	start := time.Now()
	command := cli.ParseCommandLineArgs()

	err := command.Validate()
	if err != nil {
		fmt.Println("Error in request: ", err)
	}
	err = command.Run()
	if err != nil {
		fmt.Println("Error running action: ", err)
	}

	end := time.Now()
	fmt.Println("### DURATION :: ", end.Sub(start))
}

package cli

import (
	"flag"
	"fmt"

	"github.com/phillipfriedelj/wiki-processor/cmd/internal/psql"
)

type Command struct {
	Action     string
	Path       string
	IsDir      bool
	MaxEntries int
}

func ParseCommandLineArgs() Command {
	actionPtr := flag.String("action", "none", "The action you wish the processor to perform.")
	pathPtr := flag.String("path", "none", "The path to the files you wish to process.")
	isDirPts := flag.Bool("isDir", false, "True if the file points to a directory, false if it points to a file.")
	maxEntriesPts := flag.Int("maxEntries", 0, "The max number of entries the new files can have")

	flag.Parse()

	return Command{Action: *actionPtr, Path: *pathPtr, IsDir: *isDirPts, MaxEntries: *maxEntriesPts}
}

func (c *Command) Validate() error {
	switch c.Action {
	case "split-file":
		return c.ValidateFileSplit()
	case "export-articles-from-json":
		return c.ValidateExportArticlesFromJson()
	default:
		fmt.Printf("Validated %+v\n", c)
		return nil
	}
}

func (c *Command) Run() error {
	switch c.Action {
	case "split-file":
		return c.RunFileSplit()
	case "export-articles-from-json":
		mysqlDb, err := psql.Connect()
		if err != nil {
			return err
		}
		defer mysqlDb.Close()
		return nil
	default:
		fmt.Printf("Validated %+v\n", c)
		return nil
	}
}

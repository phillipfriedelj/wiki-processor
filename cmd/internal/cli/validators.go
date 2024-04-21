package cli

import (
	"errors"
	"os"
)

// TODO Check if path is file and valid
func (c *Command) ValidateFileSplit() error {
	if c.MaxEntries <= 1 {
		return errors.New("the split factor has to be bigger than one")
	}
	fileInfo, err := os.Stat(c.Path)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("the provided path could not be found")
	}
	if c.IsDir && !fileInfo.Mode().IsDir() {
		return errors.New("the provided path is not a directory but the isDir flag is set to true")
	}
	return nil
}

func (c *Command) ValidateExportArticlesFromJson() error {
	if c.Path == "" {
		return errors.New("a path needs to be defined for this action")
	}
	return nil
}

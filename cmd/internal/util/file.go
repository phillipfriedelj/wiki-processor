package util

import (
	"fmt"
	"io/fs"
	"os"
)

func GetFilesInDir(path string) ([]fs.DirEntry, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		fmt.Println("Error getting files: ", err)
		return nil, err
	}

	return files, nil
}

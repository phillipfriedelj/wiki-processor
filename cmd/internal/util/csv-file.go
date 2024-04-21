package util

import (
	"encoding/csv"
	"fmt"
	"os"
)

func OpenCSVFile(path string) (*os.File, *csv.Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening csv file: ", err)
		return nil, nil, err
	}

	reader := csv.NewReader(file)
	return file, reader, nil
}

package util

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"
)

func OpenJsonFile(path string) (*os.File, *json.Decoder, error) {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening file: ", err)
		return nil, nil, err
	}

	decoder := json.NewDecoder(file)
	return file, decoder, nil
}

// TODO Make generic
func WriteJsonFile(path string, data []domain.JsonArticle) error {
	file, err := os.Create(path)
	if err != nil {
		fmt.Println("Error creating json file: ", err)
		return err
	}

	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		fmt.Println("Error encoding JSON data: ", err)
		return err
	}

	return nil
}

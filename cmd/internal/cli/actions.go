package cli

import (
	"fmt"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"
	"github.com/phillipfriedelj/wiki-processor/cmd/internal/util"
)

// TODO MAKE GENERIC
// TODO Create folder for output
func (c *Command) RunFileSplit() error {
	fmt.Println("Starting to split files...")
	if c.IsDir {
		files, err := util.GetFilesInDir(c.Path)
		if err != nil {
			fmt.Println("ERROR GETTING FILES", err)
			return err
		}

		var wg sync.WaitGroup
		errors := make(chan error, len(files))

		for _, file := range files {
			if !file.IsDir() {
				wg.Add(1)
				go func(file fs.DirEntry) {
					defer wg.Done()
					_, err := splitJsonFile(path.Join(c.Path, file.Name()), c.MaxEntries)
					if err != nil {
						errors <- err
					}
				}(file)
			}
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		_, err := splitJsonFile(c.Path, c.MaxEntries)
		return err
	}
}

func splitJsonFile(jsonPath string, maxEntries int) (int, error) {
	file, decoder, err := util.OpenJsonFile(jsonPath)
	if err != nil {
		fmt.Println("ERROR OPENING FILE", err)
		return 0, err
	}
	defer file.Close()

	_, err = decoder.Token()
	if err != nil {
		fmt.Printf("[%s] ERROR DECODING TOKEN: %v", file.Name(), err)
	}

	var entries []domain.JsonArticle
	writtenFiles := 0
	for decoder.More() {
		var article domain.JsonArticle
		err := decoder.Decode(&article)
		if err != nil {
			if err == io.EOF {
				if len(entries) > 0 {
					oldFileName := strings.Split(filepath.Base(file.Name()), ".")[0]
					splitFileName := oldFileName + "_" + strconv.Itoa(writtenFiles) + ".json"
					pathWithoutName, _ := filepath.Split(jsonPath)
					splitFilePath := path.Join(pathWithoutName, "/split/", splitFileName)

					err = util.WriteJsonFile(splitFilePath, entries)
					if err != nil {
						fmt.Println("ERROR WRITING TO FILE", err)
						return 0, err
					}
					writtenFiles++
					entries = nil
				}
				break
			}
			fmt.Println("Error decoding json object: ", err)
			continue
		}
		entries = append(entries, article)

		if len(entries) >= maxEntries {
			oldFileName := strings.Split(filepath.Base(file.Name()), ".")[0]
			splitFileName := oldFileName + "_" + strconv.Itoa(writtenFiles) + ".json"
			pathWithoutName, _ := filepath.Split(jsonPath)
			splitFilePath := path.Join(pathWithoutName, "/split/", splitFileName)

			err = util.WriteJsonFile(splitFilePath, entries)
			if err != nil {
				fmt.Println("ERROR WRITING TO FILE", err)
				return 0, err
			}

			writtenFiles++
			entries = nil
		}
	}

	return writtenFiles, nil
}

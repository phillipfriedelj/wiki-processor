package cli

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/lib/pq"
	"github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"
	"github.com/phillipfriedelj/wiki-processor/cmd/internal/psql"
	"github.com/phillipfriedelj/wiki-processor/cmd/internal/repository"
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
		fmt.Println("Files have been split succesfully")
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

// TODO Split into csv and json file and add flag
func (c *Command) ExportCategoriesJson() error {
	psqlDb, err := psql.Connect()
	if err != nil {
		return err
	}
	defer psqlDb.Close()

	wikiRepo := repository.NewPsqlWikiRepository(psqlDb)

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
					err := extractAndStoreCategoriesJson(path.Join(c.Path, file.Name()), c.MaxEntries, &wikiRepo)
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
		fmt.Println("Categories have been stored in the db succesfully")
		return nil
	} else {
		err := extractAndStoreCategoriesJson(c.Path, c.MaxEntries, &wikiRepo)
		if err != nil {
			return err
		}
		fmt.Println("Categories have been stored in the db succesfully")
		return nil
	}
}

func extractAndStoreCategoriesJson(path string, maxEntries int, wikiRepo repository.WikiRepository) error {

	file, decoder, err := util.OpenJsonFile(path)
	if err != nil {
		fmt.Println("Error opening csv file: ", err)
	}
	defer file.Close()

	// _, err = decoder.Token()
	// if err != nil {
	// 	fmt.Printf("[%s] ERROR DECODING TOKEN: %v", file.Name(), err)
	// }

	for decoder.More() {
		var categories []string
		err := decoder.Decode(&categories)
		if err != nil {
			if err == io.EOF {
				// if len(categories) > 0 {
				// 	err := wikiRepo.CreateCategoriesBulk(categories)
				// 	if err != nil {
				// 		// Check if the error is due to duplicate key violation
				// 		pgErr, ok := err.(*pq.Error)
				// 		if ok && pgErr.Code == "23505" { // Postgres error code for unique violation
				// 			// Handle duplicate key violation
				// 			fmt.Println(fmt.Sprintf("[%s] Duplicate key violation: %s\n", path, err))
				// 		} else {
				// 			// Handle other errors
				// 			fmt.Println(fmt.Sprintf("[%s] Error performing bulk insert: %s\n", path, err))
				// 			return err // or continue processing, depending on your requirements
				// 		}
				// 		categories = nil
				// 	}
				// }
				break
			}
			fmt.Println("Error decoding json object: ", err)
			continue
		}
		// fmt.Printf("CATS: %+v\n", categories)
		var categoryObjects []domain.JsonCategory
		for _, category := range categories {
			firstLetter := strings.Trim(string(category[0]), "\"")
			categoryObjects = append(categoryObjects, domain.JsonCategory{Title: category, FirstLetter: firstLetter})
		}

		chunkedCategories := ChunkSlice(categoryObjects, 25000)
		for _, chunk := range chunkedCategories {
			err = wikiRepo.CreateCategoriesBulk(chunk)
			if err != nil {
				// Check if the error is due to duplicate key violation
				pgErr, ok := err.(*pq.Error)
				if ok && pgErr.Code == "23505" { // Postgres error code for unique violation
					// Handle duplicate key violation
					fmt.Println(fmt.Sprintf("[%s] Duplicate key violation: %s\n", path, err))
				} else {
					// Handle other errors
					fmt.Println(fmt.Sprintf("[%s] Error performing bulk insert: %s\n", path, err))
					return err // or continue processing, depending on your requirements
				}
			}
		}
		fmt.Println("INSERTED CATS FOR ", file.Name())

		// if len(categories) >= maxEntries {
		// err := wikiRepo.CreateCategoriesBulk(categories)
		// if err != nil {
		// 	// Check if the error is due to duplicate key violation
		// 	pgErr, ok := err.(*pq.Error)
		// 	if ok && pgErr.Code == "23505" { // Postgres error code for unique violation
		// 		// Handle duplicate key violation
		// 		fmt.Println(fmt.Sprintf("[%s] Duplicate key violation: %s\n", path, err))
		// 	} else {
		// 		// Handle other errors
		// 		fmt.Println(fmt.Sprintf("[%s] Error performing bulk insert: %s\n", path, err))
		// 		return err // or continue processing, depending on your requirements
		// 	}
		// 	categories = nil
		// }
		// }
	}
	// for {
	// 	record, err := decoder.Read()
	// 	if err == io.EOF {
	// 		if len(categories) > 0 {
	// 			err := wikiRepo.CreateCategoriesBulk(categories)
	// 			if err != nil {
	// 				fmt.Println("Error performing bulk insert: ", err)
	// 				return err
	// 			}
	// 			categories = nil
	// 		}
	// 		break
	// 	}
	// 	if err != nil {
	// 		fmt.Printf("error reading CSV record: %v", err)
	// 		return err
	// 	}

	// 	// Append the value from the first column to the data slice
	// 	title := strings.Trim(string(record[0]), "\"")
	// 	firstLetter := string(title[0])

	// 	category := domain.JsonCategory{Title: title, FirstLetter: firstLetter}
	// 	categories = append(categories, category)

	// 	if len(categories) >= maxEntries {
	// 		err := wikiRepo.CreateCategoriesBulk(categories)
	// 		if err != nil {
	// 			// Check if the error is due to duplicate key violation
	// 			pgErr, ok := err.(*pq.Error)
	// 			if ok && pgErr.Code == "23505" { // Postgres error code for unique violation
	// 				// Handle duplicate key violation
	// 				fmt.Println(fmt.Sprintf("[%s] Duplicate key violation: %s\n", path, err))
	// 			} else {
	// 				// Handle other errors
	// 				fmt.Println(fmt.Sprintf("[%s] Error performing bulk insert: %s\n", path, err))
	// 				return err // or continue processing, depending on your requirements
	// 			}
	// 			categories = nil
	// 		}
	// 		// err := wikiRepo.CreateCategoriesBulk(categories)
	// 		// if err != nil {
	// 		// 	fmt.Println(fmt.Sprintf("[%s] Error performing bulk insert: %s\n", path, err))
	// 		// 	return err
	// 		// }
	// 		// categories = nil
	// 	}
	// }

	return nil
}

func ChunkSlice(slice []domain.JsonCategory, chunkSize int) [][]domain.JsonCategory {
	var chunks [][]domain.JsonCategory

	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}

	return chunks
}

func (c *Command) ExportArticlesJson() error {
	psqlDb, err := psql.Connect()
	if err != nil {
		return err
	}
	defer psqlDb.Close()

	wikiRepo := repository.NewPsqlWikiRepository(psqlDb)

	if c.IsDir {
		files, err := util.GetFilesInDir(c.Path)
		if err != nil {
			fmt.Println("ERROR GETTING FILES", err)
			return err
		}

		for _, file := range files {
			fmt.Println("FILE :: ", file.Name())
		}

		var wg sync.WaitGroup
		errors := make(chan error, len(files))

		for _, file := range files {
			if !file.IsDir() {
				wg.Add(1)
				go func(file fs.DirEntry) {
					defer wg.Done()
					err := extractAndStoreArticles(path.Join(c.Path, file.Name()), &wikiRepo)
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
		fmt.Println("Articles and categories successfully inserted and associated!")
		return nil

	} else {
		err := extractAndStoreArticles(c.Path, &wikiRepo)
		return err
	}
}

func extractAndStoreArticles(path string, wikiRepo repository.WikiRepository) error {
	fmt.Println("EXTRCTING -- ", path)
	// defer wg.Done()
	file, decoder, err := util.OpenJsonFile(path)
	if err != nil {
		fmt.Println("Error opening json file: ", err)
		return err
	}
	defer file.Close()

	t, err := decoder.Token()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%T: %v\n", t, t)

	for decoder.More() {
		// if pageCount >= MAX_COUNT {
		// 	break
		// }
		var article domain.JsonArticle
		err := decoder.Decode(&article)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error decoding json object: ", err)
			continue
		}

		//Insert article into articles table
		articleID, err := wikiRepo.CreateArticle(&article)
		if err != nil {
			log.Println(err)
			continue
		}

		// Associate article with categories in categories_articles table
		err = wikiRepo.AssociateCategories(articleID, article.Categories)
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Printf("Article '%s' inserted successfully\n", article.Title)

	}

	return nil

}

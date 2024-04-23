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

		chunkedCategories := chunkSlice(categoryObjects, 25000)
		for _, chunk := range chunkedCategories {
			_, err = wikiRepo.CreateCategoriesBulk(chunk)
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

	}
	return nil
}

func chunkSlice(slice []domain.JsonCategory, chunkSize int) [][]domain.JsonCategory {
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

		// for _, file := range files {
		// 	fmt.Println("FILE :: ", file.Name())
		// }

		// errors := make(chan error)
		// errProcess := new(sync.WaitGroup)
		// errProcess.Add(1)
		// go func() {
		// 	for err := range errors {
		// 		fmt.Println("ERR ", err)
		// 	}
		// 	errProcess.Done()
		// }()

		// Define the number of workers
		numWorkers := 85

		// Create channels for tasks and results
		tasks := make(chan domain.JsonArticle, numWorkers)
		results := make(chan error, numWorkers)
		// taskProcess := new(sync.WaitGroup)

		// Start worker goroutines
		for i := 0; i < numWorkers; i++ {
			go storeArticlesWorker(&wikiRepo, tasks, results)
		}

		for _, file := range files {
			if !file.IsDir() {
				// taskProcess.Add(1)
				go func(file fs.DirEntry) {
					// defer taskProcess.Done()
					err := extractJsonArticles(path.Join(c.Path, file.Name()), tasks)
					if err != nil {
						results <- err
					}
				}(file)
			}
		}

		// Wait for all tasks to be completed
		go func() {
			for i := 0; i < len(files); i++ {
				if err := <-results; err != nil {
					fmt.Println("ERROR:", err)
				}
			}
			close(tasks)
		}()

		// Wait for all workers to finish
		for i := 0; i < numWorkers; i++ {
			<-results
		}

		// storeProcess := new(sync.WaitGroup)
		// storeProcess.Add(1)
		// go func() {
		// 	defer storeProcess.Done()
		// 	var batch []domain.JsonArticle
		// 	count := 0
		// 	for task := range tasks {
		// 		batch = append(batch, task)
		// 		if len(batch) >= c.MaxEntries {
		// 			count++

		// 			err := storeArticles(&wikiRepo, batch)
		// 			batch = nil
		// 			errors <- err
		// 		}
		// 	}

		// 	close(errors)
		// }()

		// taskProcess.Wait()
		// fmt.Println("FINISHED PROCESSING")
		// close(tasks)
		// taskProcess.Wait()

		// writeProcess.Wait()
		// errProcess.Wait()

		// for err := range errors {
		// 	if err != nil {
		// 		return err
		// 	}
		// }
		fmt.Println("Articles and categories successfully inserted and associated!")
		return nil

	} else {
		//TODO Figure out for single file
		//err := extractAndStoreArticles(c.Path, &wikiRepo)
		return nil
	}
}

func extractJsonArticles(path string, taskChan chan domain.JsonArticle) error {
	file, decoder, err := util.OpenJsonFile(path)
	if err != nil {
		fmt.Println("Error opening json file: ", err)
		return err
	}
	defer file.Close()

	_, err = decoder.Token()
	if err != nil {
		fmt.Println(err)
		return err
	}

	//TODO Batch send to channel
	for decoder.More() {
		var article domain.JsonArticle
		err := decoder.Decode(&article)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error decoding json object: ", err)
			continue
		}

		taskChan <- article
	}

	return nil
}

func storeArticles(wikiRepo repository.WikiRepository, articles []domain.JsonArticle) error {
	for _, article := range articles {
		if len(article.Categories) > 0 {

			newCategories := article.Categories

			articleId, err := wikiRepo.CreateArticle(&article)
			if err != nil {
				fmt.Println("ERROR CREATING ARTICLE: ", err)
				return err
			}

			results, err := wikiRepo.GetExistingCategories(article.Categories)
			if err != nil {
				fmt.Println("ERROR GETTING EXISTING CATEGORIES: ", err)
				break
			}

			existingCategories := results
			for _, result := range results {
				newCategories = remove(newCategories, result.Title)
			}

			//TODO Reduce for loops
			var newCategoryObjects []domain.JsonCategory
			for _, newCategory := range newCategories {
				newCategoryObjects = append(newCategoryObjects, domain.JsonCategory{Title: newCategory, FirstLetter: string(newCategory[0])})
			}

			categoryIds, err := wikiRepo.CreateCategoriesBulk(newCategoryObjects)
			if err != nil {
				fmt.Println("ERROR BULK INSERTING NEW CATEGORIES: ", err)
				break
			}

			for _, existingCategory := range existingCategories {
				categoryIds = append(categoryIds, existingCategory.Id)
			}

			err = wikiRepo.BulkInsertCategoriesArticles(articleId, categoryIds)
			if err != nil {
				fmt.Println("ERROR BULK INSERTING ARTICLE AND CATEGORY REFERENCES: ", err)
				break
			}

			fmt.Println("STORED SUCCESSFULLY ARTICLE -- ", article.Title, " -ID: ", articleId)
		}
	}

	return nil

}

func storeArticlesWorker(wikiRepo repository.WikiRepository, tasks <-chan domain.JsonArticle, results chan<- error) {
	for article := range tasks {
		if len(article.Categories) > 0 {

			newCategories := article.Categories

			articleId, err := wikiRepo.CreateArticle(&article)
			if err != nil {
				fmt.Println("ERROR CREATING ARTICLE: ", err)
				results <- err
			}

			categoryResults, err := wikiRepo.GetExistingCategories(article.Categories)
			if err != nil {
				fmt.Println("ERROR GETTING EXISTING CATEGORIES: ", err)
				results <- err
				break
			}

			existingCategories := categoryResults
			for _, result := range categoryResults {
				newCategories = remove(newCategories, result.Title)
			}

			//TODO Reduce for loops
			var newCategoryObjects []domain.JsonCategory
			for _, newCategory := range newCategories {
				newCategoryObjects = append(newCategoryObjects, domain.JsonCategory{Title: newCategory, FirstLetter: string(newCategory[0])})
			}

			categoryIds, err := wikiRepo.CreateCategoriesBulk(newCategoryObjects)
			if err != nil {
				fmt.Println("ERROR BULK INSERTING NEW CATEGORIES: ", err)
				results <- err
				break
			}

			for _, existingCategory := range existingCategories {
				categoryIds = append(categoryIds, existingCategory.Id)
			}

			err = wikiRepo.BulkInsertCategoriesArticles(articleId, categoryIds)
			if err != nil {
				fmt.Println("ERROR BULK INSERTING ARTICLE AND CATEGORY REFERENCES: ", err)
				results <- err
				break
			}

			fmt.Println("STORED SUCCESSFULLY ARTICLE -- ", article.Title, " -ID: ", articleId)
		}
	}
}

func remove(slice []string, item string) []string {
	newSlice := make([]string, 0)
	for _, v := range slice {
		if v != item {
			newSlice = append(newSlice, v)
		}
	}
	return newSlice
}

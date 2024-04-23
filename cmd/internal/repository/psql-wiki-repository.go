package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"
)

type PsqlConnection struct {
	db *sql.DB
}

func NewPsqlWikiRepository(db *sql.DB) PsqlConnection {
	return PsqlConnection{db: db}
}

func (c *PsqlConnection) CreateArticle(article *domain.JsonArticle) (int, error) {
	var articleID int
	firstLetter := strings.ToLower(string(article.Title[0]))
	err := c.db.QueryRow("INSERT INTO articles (title, first_letter) VALUES ($1, $2) RETURNING id", article.Title, firstLetter).Scan(&articleID)
	if err != nil {
		return 0, err
	}
	return articleID, nil
}

func (c *PsqlConnection) CreateCategory(newCategory domain.SqlCategory) (int, error) {
	if newCategory.Title == "" || newCategory.FirstLetter == "" {
		fmt.Println("-----invalid request: not all fields were set")
		return 0, errors.New("invalid request: not all fields were set")
	}
	var catId int
	err := c.db.QueryRow("INSERT INTO categories(title, first_letter) VALUES ($1,$2) RETURNING id", newCategory.Title, newCategory.FirstLetter).Scan(&catId)
	if err != nil {
		return 0, err
	}
	return catId, nil
}

func (c *PsqlConnection) CreateCategoriesBulk(categories []domain.JsonCategory) ([]int, error) {
	// Begin a transaction
	tx, err := c.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Prepare the statement with RETURNING clause
	stmt, err := tx.Prepare("INSERT INTO categories (title, first_letter) VALUES ($1, $2) RETURNING id")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Execute the bulk insert
	var ids []int
	for _, category := range categories {
		var categoryID int
		err := stmt.QueryRow(category.Title, category.FirstLetter).Scan(&categoryID)
		if err != nil {
			return nil, err
		}
		ids = append(ids, categoryID)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return ids, nil
}

func (c *PsqlConnection) GetAllCategoriesByLetter(letter string) ([]domain.SqlCategory, error) {
	rows, err := c.db.Query("SELECT * FROM categories WHERE first_letter=$1", letter)
	if err != nil {
		return nil, err
	}

	categories := make([]domain.SqlCategory, 0)
	for rows.Next() {
		var category domain.SqlCategory
		if err := rows.Scan(&category); err != nil {
			return nil, err
		}
		categories = append(categories, category)
	}

	return categories, nil
}

func (c *PsqlConnection) AssociateCategories(articleID int, categories []string) error {

	for _, category := range categories {
		var categoryID int
		err := c.db.QueryRow("SELECT id FROM categories WHERE title = $1", category).Scan(&categoryID)
		if err != nil {
			if err == sql.ErrNoRows {
				// Category does not exist, insert it
				firstLetter := strings.ToLower(string(category[0]))
				categoryID, err = c.CreateCategory(domain.SqlCategory{Title: category, FirstLetter: firstLetter})
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		// Insert into categories_articles table
		_, err = c.db.Exec("INSERT INTO categories_articles (category_id, article_id) VALUES ($1, $2)", categoryID, articleID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *PsqlConnection) GetExistingCategories(categories []string) ([]domain.SqlCategory, error) {
	// Create a query to fetch existing categories from the database
	query := "SELECT id, title FROM categories WHERE title IN ("
	placeholders := make([]string, len(categories))
	args := make([]interface{}, len(categories))
	for i, cat := range categories {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = cat
	}
	query += strings.Join(placeholders, ", ") + ")"
	// Execute the query
	rows, err := c.db.Query(query, args...)
	if err != nil {
		fmt.Println("ERROR EXISTING CATS :: ", categories)
		fmt.Println("ERROR EXISTING STMT :: ", query)
		return nil, err
	}
	defer rows.Close()

	// Iterate over the result set
	var results []domain.SqlCategory
	for rows.Next() {
		var category domain.SqlCategory
		err := rows.Scan(&category.Id, &category.Title)
		if err != nil {
			return nil, err
		}
		results = append(results, category)
	}

	return results, nil
}

func (c *PsqlConnection) BulkInsertCategoriesArticles(articleID int, categoryIDs []int) error {
	// Begin a transaction
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare the INSERT statement with multiple rows
	valueStrings := make([]string, 0, len(categoryIDs))
	valueArgs := make([]any, 0, len(categoryIDs)*2)
	for i, categoryId := range categoryIDs {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, categoryId)
		valueArgs = append(valueArgs, articleID)
	}
	stmt := fmt.Sprintf("INSERT INTO categories_articles (category_id, article_id) VALUES %s", strings.Join(valueStrings, ","))
	// Execute the bulk insert
	_, err = tx.Exec(stmt, valueArgs...)
	if err != nil {
		return err
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

package repository

import (
	"database/sql"
	"strings"

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

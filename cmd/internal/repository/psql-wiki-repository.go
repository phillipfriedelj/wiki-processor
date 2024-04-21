package repository

import (
	"database/sql"
	"strings"

	"github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"
)

var psqlDb *sql.DB

func NewPsqlWikiRepository(db *sql.DB) {
	psqlDb = db
}

func CreateArticle(article *domain.JsonArticle) (int, error) {
	var articleID int
	firstLetter := strings.ToLower(string(article.Title[0]))
	err := psqlDb.QueryRow("INSERT INTO articles (title, first_letter) VALUES ($1, $2) RETURNING id", article.Title, firstLetter).Scan(&articleID)
	if err != nil {
		return 0, err
	}
	return articleID, nil
}

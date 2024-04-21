package repository

import "github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"

type ArticleRepository interface {
	CreateArticle(article domain.JsonArticle) (int, error)
}

type CategoryRepository interface {
	CreateCategoriesBulk(category []domain.JsonCategory) error
	GetAllCategoriesByLetter(letter string) ([]domain.SqlCategory, error)
}

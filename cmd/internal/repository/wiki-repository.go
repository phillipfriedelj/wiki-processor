package repository

import "github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"

type WikiRepository interface {
	CreateArticle(article *domain.JsonArticle) (int, error)
	CreateCategory(newCategory domain.SqlCategory) (int, error)
	CreateCategoriesBulk(category []domain.JsonCategory) error
	GetAllCategoriesByLetter(letter string) ([]domain.SqlCategory, error)
	AssociateCategories(articleID int, categories []string) error
}

package repository

import "github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"

//TODO Make all SQL
type WikiRepository interface {
	CreateArticle(article *domain.JsonArticle) (int, error)
	CreateCategory(newCategory domain.SqlCategory) (int, error)
	CreateCategoriesBulk(categories []domain.JsonCategory) ([]int, error)
	GetAllCategoriesByLetter(letter string) ([]domain.SqlCategory, error)
	AssociateCategories(articleID int, categories []string) error
	GetExistingCategories(categories []string) ([]domain.SqlCategory, error)
	BulkInsertCategoriesArticles(articleID int, categoryIDs []int) error
}

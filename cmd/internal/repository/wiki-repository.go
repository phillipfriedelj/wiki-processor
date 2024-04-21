package repository

import "github.com/phillipfriedelj/wiki-processor/cmd/internal/domain"

type WikiRepository interface {
	CreateArticle(article domain.JsonArticle) error
}

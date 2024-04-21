package domain

type JsonArticle struct {
	Id         int      `json:"id"`
	Namespace  int      `json:"Namespace"`
	Title      string   `json:"title"`
	Categories []string `json:"Categories"`
}

type SqlArticle struct {
	Id          int
	Title       string
	FirstLetter string
}

package domain

type JsonCategory struct {
	Id          int    `json:"id"`
	Title       string `json:"title"`
	FirstLetter string `json:"first_letter"`
}

type SqlCategory struct {
	Id          int
	Title       string
	FirstLetter string
}

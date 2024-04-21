CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) UNIQUE NOT NULL,
    first_letter CHAR(1) NOT NULL
);

CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) UNIQUE NOT NULL,
    first_letter CHAR(1) NOT NULL
);

CREATE TABLE categories_articles (
    category_id INT REFERENCES categories(id) NOT NULL,
    article_id INT REFERENCES articles(id) NOT NULL,
    PRIMARY KEY (category_id, article_id)
);
package psql

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func Connect() (*sql.DB, error) {
	godotenv.Load()

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("PSQL_HOST"), os.Getenv("PSQL_PORT"), os.Getenv("PSQL_USER"), os.Getenv("PSQL_PSSWD"), os.Getenv("PSQL_DBNAME"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Invalid DB config: ", err)
		return nil, err
	}

	db.SetConnMaxLifetime(time.Hour)
	db.SetMaxIdleConns(100)
	db.SetMaxOpenConns(100)

	if err = db.Ping(); err != nil {
		log.Fatal("DB unreachable: ", err)
		return nil, err
	}

	return db, err
}

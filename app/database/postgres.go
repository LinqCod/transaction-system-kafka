package database

import (
	"database/sql"
	"fmt"
	"log"
)

func NewClient(host, port, user, password, dbname string) (*sql.DB, error) {
	pgInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", pgInfo)
	if err != nil {
		return nil, fmt.Errorf("validation of db parameters failed due to error: %v", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to open db connection due to err: %v", err)
	}

	log.Println("postgres db connected successfully!")
	return db, nil
}

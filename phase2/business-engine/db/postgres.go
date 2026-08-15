package db


import (
   "context"
   "fmt"
   "os"
   "github.com/jackc/pgx/v5/pgxpool"

)

var Pool *pgxpool.Pool

func Connect() error {
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		connString = "postgres://admin:admin@localhost:5432/business_database?sslmode=disable"
	}
   var err error
   Pool , err = pgxpool.New(context.Background(), connString) 
   if err != nil {
       return fmt.Errorf("failed to create connection pool: %w", err)
   }
 

   fmt.Println("Connected to the database successfully.")
   return nil
}
func Close() {
	if Pool != nil {
		Pool.Close()
	}
}
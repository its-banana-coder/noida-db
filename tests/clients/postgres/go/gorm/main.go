// GORM against noida-db: AutoMigrate, associations, Joins, transactions and
// constraint violations.
//
// Run with PGPORT pointing at noida-db (or a real Postgres, which must pass
// just the same).
package main

import (
	"fmt"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var checks int

func check(cond bool, msg string) {
	if !cond {
		panic("FAIL: " + msg)
	}
	checks++
}

type Author struct {
	ID    uint `gorm:"primaryKey"`
	Name  string `gorm:"uniqueIndex;size:60"`
	Books []Book
}

type Book struct {
	ID       uint `gorm:"primaryKey"`
	Title    string
	Price    float64
	AuthorID uint
	Author   Author
}

func main() {
	port := os.Getenv("PGPORT")
	dsn := fmt.Sprintf("host=127.0.0.1 port=%s user=postgres dbname=postgres sslmode=disable", port)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	check(err == nil, fmt.Sprintf("open: %v", err))

	check(db.Migrator().DropTable(&Book{}, &Author{}) == nil, "drop tables")
	check(db.AutoMigrate(&Author{}, &Book{}) == nil, "automigrate")

	a := Author{Name: "Ann"}
	check(db.Create(&a).Error == nil, "create author")
	check(db.Create(&[]Book{
		{Title: "Alpha", Price: 9.99, AuthorID: a.ID},
		{Title: "Beta", Price: 19.5, AuthorID: a.ID},
	}).Error == nil, "create books")

	var count int64
	db.Model(&Book{}).Count(&count)
	check(count == 2, fmt.Sprintf("count %d", count))

	var withAuthor []Book
	check(db.Joins("Author").Where("books.price > ?", 10).Find(&withAuthor).Error == nil, "joins")
	check(len(withAuthor) >= 1, fmt.Sprintf("joins result %d", len(withAuthor)))

	err = db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&Book{}).Where("title = ?", "Alpha").Update("price", 29.99).Error
	})
	check(err == nil, fmt.Sprintf("transaction: %v", err))
	var b Book
	db.Where("title = ?", "Alpha").First(&b)
	check(b.Price == 29.99, fmt.Sprintf("updated price %v", b.Price))

	dup := Author{Name: "Ann"}
	check(db.Create(&dup).Error != nil, "unique violation should error")

	check(db.Migrator().DropTable(&Book{}, &Author{}) == nil, "drop tables (cleanup)")

	fmt.Printf("gorm: %d checks passed\n", checks)
}

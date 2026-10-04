// Go database/sql (go-sql-driver/mysql, binary-protocol prepared
// statements) and GORM (AutoMigrate, CRUD, transactions) against noida-db.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var pass, fail int

func check(name string, got, want any) {
	if reflect.DeepEqual(got, want) {
		pass++
	} else {
		fail++
		fmt.Printf("  FAIL %s\n    got  %#v\n    want %#v\n", name, got, want)
	}
}

type Product struct {
	ID        uint   `gorm:"primaryKey"`
	Code      string `gorm:"uniqueIndex;size:20"`
	Price     float64
	Stock     int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func main() {
	port := os.Getenv("NOIDA_MYSQL_PORT")
	if port == "" {
		port = "3306"
	}
	dsn := "root@tcp(127.0.0.1:" + port + ")/test?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	if _, err := db.Exec("CREATE TABLE g (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20), qty INT, at DATETIME, price DECIMAL(6,2))"); err != nil {
		panic(err)
	}
	res, err := db.Exec("INSERT INTO g (name, qty, at, price) VALUES (?, ?, ?, ?)", "a", 3, time.Date(2024, 3, 5, 14, 7, 9, 0, time.UTC), "9.99")
	check("insert err", err, nil)
	if res != nil {
		id, _ := res.LastInsertId()
		check("LastInsertId", id, int64(1))
	}
	_, err = db.Exec("INSERT INTO g (name, qty) VALUES (?, ?)", "b", nil)
	check("insert NULL err", err, nil)
	var (
		name  string
		qty   sql.NullInt64
		at    sql.NullTime
		price sql.NullString
	)
	err = db.QueryRow("SELECT name, qty, at, price FROM g WHERE id = ?", 1).Scan(&name, &qty, &at, &price)
	check("scan err", err, nil)
	check("typed scan", []any{name, qty.Int64, at.Time.UTC().Format(time.RFC3339), price.String}, []any{"a", int64(3), "2024-03-05T14:07:09Z", "9.99"})
	db.QueryRow("SELECT qty FROM g WHERE id = ?", 2).Scan(&qty)
	check("NULL int", qty.Valid, false)
	var n int
	db.QueryRow("SELECT COUNT(*) FROM g WHERE name LIKE ? LIMIT ?", "%", 10).Scan(&n)
	check("count with LIMIT ?", n, 2)
	stmt, _ := db.Prepare("SELECT ? + qty FROM g WHERE id = ?")
	var v int64
	for i := 0; i < 5; i++ {
		stmt.QueryRow(i, 1).Scan(&v)
	}
	check("prepared reuse", v, int64(7))
	tx, _ := db.Begin()
	tx.Exec("DELETE FROM g")
	tx.Rollback()
	db.QueryRow("SELECT COUNT(*) FROM g").Scan(&n)
	check("tx rollback", n, 2)
	tx, _ = db.Begin()
	tx.Exec("UPDATE g SET qty = ? WHERE id = ?", 42, 2)
	tx.Commit()
	db.QueryRow("SELECT qty FROM g WHERE id = ?", 2).Scan(&qty)
	check("tx commit", qty.Int64, int64(42))

	gdb, err := gorm.Open(mysql.Open(dsn+"&charset=utf8mb4"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		panic(err)
	}
	check("AutoMigrate", gdb.AutoMigrate(&Product{}), nil)
	check("AutoMigrate again", gdb.AutoMigrate(&Product{}), nil)
	p := Product{Code: "D42", Price: 100, Stock: 1}
	check("Create", gdb.Create(&p).Error, nil)
	check("ID set", p.ID, uint(1))
	var got Product
	gdb.First(&got, "code = ?", "D42")
	check("First", []any{got.Code, got.Price, got.Stock}, []any{"D42", 100.0, 1})
	gdb.Model(&got).Updates(Product{Price: 200, Stock: 0})
	gdb.First(&got, got.ID)
	check("Updates (zero value skipped)", []any{got.Price, got.Stock}, []any{200.0, 1})
	check("unique violation", gdb.Create(&Product{Code: "D42"}).Error != nil, true)
	var cnt int64
	gdb.Model(&Product{}).Where("price > ?", 150).Count(&cnt)
	check("Count", cnt, int64(1))
	gdb.Transaction(func(tx *gorm.DB) error {
		tx.Create(&Product{Code: "T1"})
		return fmt.Errorf("rollback")
	})
	gdb.Model(&Product{}).Where("code = ?", "T1").Count(&cnt)
	check("gorm Transaction rollback", cnt, int64(0))
	gdb.Delete(&got)
	gdb.Model(&Product{}).Count(&cnt)
	check("Delete", cnt, int64(0))
	fmt.Printf("go database/sql + gorm: %d/%d checks passed\n", pass, pass+fail)
	if fail > 0 {
		os.Exit(1)
	}
}

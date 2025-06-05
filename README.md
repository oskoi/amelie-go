# Amelie Driver for Go

[Amelie](https://github.com/amelielabs/amelie) Driver is is an implementation of database/sql/driver interface.

## Installing

```
go get -u github.com/oskoi/amelie-go
```

## Example

You only need to import the driver and can use the full database/sql API then.

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/oskoi/amelie-go"
)

func main() {
	db, err := sql.Open("amelie", "http://localhost:3485")
	handleErr(err, "open")

	// db := amelie.OpenDB(
	// 	"http://localhost:3485",
	// 	amelie.OptionExecutor(amelie.NewHTTPExecutor("my jwt token")),
	// )

	err = db.Ping()
	handleErr(err, "ping")

	_, err = db.Exec("create table if not exists counters (device_id int primary key, hits int)")
	handleErr(err, "create counters")

	tx, err := db.Begin()
	tx.Exec("insert into counters values (1, ?)", 10)
	tx.Exec("insert into counters values (2, ?)", 20)
	err = tx.Commit()
	handleErr(err, "commit")

	rows, err := db.Query("select * from counters where id in (?, ?) format 'json-obj'", 1, 2)
	handleErr(err, "query")

	columns, err := rows.Columns()
	handleErr(err, "query")

	for n := 0; rows.Next(); n += 1 {
		vs := make([]any, len(columns))
		for i := range vs {
			vs[i] = &vs[i]
		}

		err = rows.Scan(vs...)
		handleErr(err, "scan")

		fmt.Printf("row[%d]:\n", n)
		for i, col := range columns {
			fmt.Printf("%s = %v\n", col, vs[i])
		}
		fmt.Println()
	}
}

func handleErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}
```

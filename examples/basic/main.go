package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"sync"

	_ "github.com/oskoi/amelie-go"
)

func main() {
	ctx := context.Background()

	url := new(url.URL)
	url.Scheme = "amelie"
	url.Path = "data"
	args := url.Query()
	// args.Add("token", "12345") Authorization JSON Web Tokens
	// args.Add("remote", "")
	args.Add("format", "json-obj")
	args.Add("log_to_stdout", "false")
	args.Add("wal_worker", "false")
	args.Add("wal_sync_on_create", "false")
	args.Add("wal_sync_on_close", "false")
	args.Add("wal_sync_on_write", "false")
	args.Add("checkpoint_sync", "false")
	args.Add("frontends", "4")
	args.Add("backends", "4")
	args.Add("listen", "[]")
	url.RawQuery = args.Encode()

	db, err := sql.Open("amelie", url.String())
	// db, err := sql.Open("amelie", "http://localhost:3485")
	handleErr(err, "open")
	defer db.Close()

	err = db.Ping()
	handleErr(err, "ping")

	_, err = db.Exec("create table if not exists counters (id int primary key, hits int)")
	handleErr(err, "create counters")

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := db.Conn(ctx)
			handleErr(err, "get conn")
			defer conn.Close()

			for range 4 {
				tx, err := db.Begin()
				handleErr(err, "begin tx")
				tx.Exec("insert into counters values (?, ?) on conflict do update set hits = hits + 1", i, 10)
				tx.Exec("insert into counters values (?, ?) on conflict do update set hits = hits + 1", i, 20)
				err = tx.Commit()
				handleErr(err, "commit tx")
			}
		}()
	}
	wg.Wait()

	rows, err := db.Query("select * from counters where id in (?, ?, ?, ?)", 0, 1, 2, 3)
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

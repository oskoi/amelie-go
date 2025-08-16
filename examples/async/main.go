package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"

	"github.com/oskoi/amelie-go"
	_ "github.com/oskoi/amelie-go"
	"github.com/oskoi/amelie-go/native"
)

func main() {
	ctx := context.Background()
	db, err := sql.Open("amelie", native.URL("data", // only for linux
		"log_to_stdout", "false",
		"wal_worker", "false",
		"wal_sync_on_create", "false",
		"wal_sync_on_close", "false",
		"wal_sync_on_write", "false",
		"checkpoint_sync", "false",
		"frontends", "4",
		"backends", "4",
		"listen", "[]",
	))
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

			rs := make([]*native.RequestResult, 0, 4)
			for range 4 {
				conn.Raw(func(driverConn any) error {
					session := driverConn.(*amelie.Conn).Session().(*amelie.NativeSession)
					rs = append(rs, session.ExecuteRaw(fmt.Appendf(nil,
						"insert into counters values (%d, %d) on conflict do update set hits = hits + 1", i, 10,
					)))
					return nil
				})
			}
			native.WaitAll(rs)
		}()
	}
	wg.Wait()

	rows, err := db.Query("select * from counters where id in (?, ?, ?, ?) format 'json-obj'", 0, 1, 2, 3)
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

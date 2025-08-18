package main

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/oskoi/amelie-go/internal/server"
	"github.com/oskoi/amelie-go/native"
	"github.com/urfave/cli/v3"

	_ "github.com/oskoi/amelie-go"
)

func main() {
	cmd := &cli.Command{
		Name:  "amelie",
		Usage: "amelie cli",
		Commands: []*cli.Command{
			{
				Name: "server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "grpc_addr",
						Value: ":50051",
					},
					&cli.StringFlag{
						Name: "uuid",
					},
					&cli.StringFlag{
						Name: "timezone",
					},
					&cli.StringFlag{
						Name: "format",
					},
					&cli.BoolFlag{
						Name: "log_enable",
					},
					&cli.BoolFlag{
						Name: "log_to_file",
					},
					&cli.BoolFlag{
						Name: "log_to_stdout",
					},
					&cli.BoolFlag{
						Name: "log_connections",
					},
					&cli.BoolFlag{
						Name: "log_options",
					},
					&cli.StringFlag{
						Name: "tls_capath",
					},
					&cli.StringFlag{
						Name: "tls_ca",
					},
					&cli.StringFlag{
						Name: "tls_cert",
					},
					&cli.StringFlag{
						Name: "tls_key",
					},
					&cli.IntFlag{
						Name: "limit_send",
					},
					&cli.IntFlag{
						Name: "limit_recv",
					},
					&cli.IntFlag{
						Name: "limit_write",
					},
					&cli.IntFlag{
						Name: "frontends",
					},
					&cli.IntFlag{
						Name: "backends",
					},
					&cli.BoolFlag{
						Name: "wal_worker",
					},
					&cli.BoolFlag{
						Name: "wal_crc",
					},
					&cli.BoolFlag{
						Name: "wal_sync_on_create",
					},
					&cli.BoolFlag{
						Name: "wal_sync_on_close",
					},
					&cli.BoolFlag{
						Name: "wal_sync_on_write",
					},
					&cli.StringFlag{
						Name: "wal_sync_interval",
					},
					&cli.IntFlag{
						Name: "wal_size",
					},
					&cli.IntFlag{
						Name: "wal_truncate",
					},
					&cli.StringFlag{
						Name: "checkpoint_interval",
					},
					&cli.StringFlag{
						Name: "checkpoint_interval",
					},
					&cli.IntFlag{
						Name: "checkpoint_workers",
					},
					&cli.BoolFlag{
						Name: "checkpoint_crc",
					},
					&cli.StringFlag{
						Name: "checkpoint_compression",
					},
					&cli.BoolFlag{
						Name: "checkpoint_sync",
					},
				},
				Action: func(ctx context.Context, c *cli.Command) error {
					flagNames := c.FlagNames()
					args := make([]string, 0, len(flagNames))
					for _, name := range flagNames {
						args = append(args, fmt.Sprintf("%v", c.Value(name)))
					}

					path := cmp.Or(c.Args().First(), "data")
					db, err := sql.Open("amelie", native.URL(path, args...))
					if err != nil {
						return fmt.Errorf("open db: %w", err)
					}

					server := server.NewServer(c.String("grpc_addr"), db)
					defer server.Shutdown()

					if err := server.Run(); err != nil {
						return fmt.Errorf("server: %w", err)
					}

					return nil
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

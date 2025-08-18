package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	ameliev1 "github.com/oskoi/amelie-go/gen/pb/amelie/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type ConnTx interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func NewServer(grpcAddr string, db *sql.DB) *Server {
	grpcPanicRecoveryHandler := func(r any) (err error) {
		slog.Error("recovered", slog.Any("error", r))

		return status.Errorf(codes.Internal, "%v", r)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		),
		grpc.ChainStreamInterceptor(
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(grpcPanicRecoveryHandler)),
		),
	)

	server := &Server{
		grpcAddr:   grpcAddr,
		grpcServer: grpcServer,
		db:         db,
	}

	reflection.Register(grpcServer)
	ameliev1.RegisterAmelieServiceServer(grpcServer, server)

	return server
}

type Server struct {
	ameliev1.AmelieServiceServer

	grpcAddr   string
	grpcServer *grpc.Server
	db         *sql.DB
}

func (s *Server) Run() error {
	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer s.Shutdown()

	slog.Info(fmt.Sprintf("start listening: %s", s.grpcAddr))

	return s.grpcServer.Serve(lis)
}

func (s *Server) Shutdown() {
	slog.Info("shutdown")

	s.grpcServer.GracefulStop()
}

func (s *Server) Execute(stream grpc.BidiStreamingServer[ameliev1.ExecuteRequest, ameliev1.ExecuteResponse]) error {
	ctx := stream.Context()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		status.Errorf(codes.Internal, "conn: %v", err)
	}
	defer conn.Close()

	for {
		req, err := stream.Recv()
		if err != nil {
			return status.Errorf(codes.Internal, "receive: %v", err)
		}

		queries := req.GetQueries()
		if len(queries) == 0 {
			continue
		}

		if len(queries) == 1 {
			query := queries[0]
			querySql := strings.TrimSpace(query.GetQuery())
			queryArgs := queryArgs(query.GetArgs())

			var rows []*ameliev1.Row
			if strings.ToLower(querySql[:4]) == "with" || strings.ToLower(querySql[:6]) == "select" {
				sqlRows, err := conn.QueryContext(ctx, querySql, queryArgs...)
				if err != nil {
					return status.Errorf(codes.Internal, "receive: %v", err)
				}

				cols, err := sqlRows.Columns()
				if err != nil {
					return status.Errorf(codes.Internal, "query: %v", err)
				}

				dest := make([]any, len(cols))
				for i := range dest {
					dest[i] = &dest[i]
				}

				for n := 0; sqlRows.Next(); n += 1 {
					if err := sqlRows.Scan(dest...); err != nil {
						return status.Errorf(codes.Internal, "scan: %v", err)
					}
					rows = append(rows, &ameliev1.Row{Cols: structCols(cols, dest)})
				}
			} else {
				if _, err := conn.ExecContext(ctx, querySql, queryArgs...); err != nil {
					return status.Errorf(codes.Internal, "exec: %v", err)
				}
			}

			if err := stream.Send(&ameliev1.ExecuteResponse{Rows: rows}); err != nil {
				return status.Errorf(codes.Internal, "send: %v", err)
			}

			continue
		}

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return status.Errorf(codes.Internal, "begin tx: %v", err)
		}

		for _, query := range req.Queries {
			tx.ExecContext(ctx, query.GetQuery(), queryArgs(query.GetArgs()))
		}

		if err := tx.Commit(); err != nil {
			return status.Errorf(codes.Internal, "commit: %v", err)
		}
	}
}

func queryArgs(args []*structpb.Value) []any {
	vs := make([]any, len(args))
	for i := range args {
		vs[i] = args[i].AsInterface()
	}
	return vs
}

func structCols(cols []string, dest []any) map[string]*structpb.Value {
	vs := make(map[string]*structpb.Value)
	for i, col := range cols {
		vs[col], _ = structpb.NewValue(dest[i])
	}
	return vs
}

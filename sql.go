package amelie

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var (
	ErrUnsupportedStatement = errors.New("unsupported statement")
	ErrUnauthorized         = errors.New("unauthorized")
)

var amelieDriver *Driver

func init() {
	amelieDriver = &Driver{}
	sql.Register("amelie", amelieDriver)
}

type Executor interface {
	Execute(ctx context.Context, url string, query []byte) ([]byte, error)
}

type OptionOpenDB func(*connector)

func OptionExecutor(ex Executor) OptionOpenDB {
	return func(c *connector) {
		c.executor = ex
	}
}

func GetConnector(addr string, opts ...OptionOpenDB) driver.Connector {
	c := connector{
		addr:   addr,
		driver: amelieDriver,
	}

	for _, opt := range opts {
		opt(&c)
	}

	if c.executor == nil {
		c.executor = NewHTTPExecutor("")
	}

	return c
}

func OpenDB(addr string, opts ...OptionOpenDB) *sql.DB {
	c := GetConnector(addr, opts...)
	return sql.OpenDB(c)
}

type connector struct {
	addr     string
	executor Executor
	driver   *Driver
}

func (c connector) Connect(ctx context.Context) (driver.Conn, error) {
	return &Conn{
		addr:     c.addr,
		execUrl:  c.addr + "/v1/execute",
		executor: c.executor,
		driver:   c.driver,
	}, nil
}

func (c connector) Driver() driver.Driver {
	return c.driver
}

func GetDefaultDriver() driver.Driver {
	return amelieDriver
}

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	return connector.Connect(ctx)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &driverConnector{
		addr:     name,
		executor: NewHTTPExecutor(""),
		driver:   d,
	}, nil
}

type driverConnector struct {
	addr     string
	executor Executor
	driver   *Driver
}

func (dc *driverConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return &Conn{
		addr:     dc.addr,
		execUrl:  dc.addr + "/v1/execute",
		executor: dc.executor,
		driver:   dc.driver,
	}, nil
}

func (dc *driverConnector) Driver() driver.Driver {
	return dc.driver
}

type Conn struct {
	addr     string
	execUrl  string
	executor Executor
	driver   *Driver
	closed   bool
	activeTx *sqlTx
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, ErrUnsupportedStatement
}

func (c *Conn) Close() error {
	c.closed = true

	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	tx := newSqlTx(ctx, c)
	tx.Begin()

	c.activeTx = tx

	return tx, nil
}

func (c *Conn) ExecContext(
	ctx context.Context,
	query string,
	argsV []driver.NamedValue,
) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	queryBs, err := compileQuery(query, argsV)
	if err != nil {
		return nil, fmt.Errorf("compile query: %w", err)
	}

	if c.activeTx != nil {
		c.activeTx.AppendQuery(queryBs)

		return nil, nil
	}

	if _, err := c.executor.Execute(ctx, c.execUrl, queryBs); err != nil {
		return nil, fmt.Errorf("execute: %w", err)
	}

	return nil, nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, argsV []driver.NamedValue) (driver.Rows, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	queryBs, err := compileQuery(query, argsV)
	if err != nil {
		return nil, fmt.Errorf("compile query: %w", err)
	}

	if c.activeTx != nil {
		return nil, ErrUnsupportedStatement
	}

	data, err := c.executor.Execute(ctx, c.execUrl, queryBs)
	if err != nil {
		return nil, fmt.Errorf("execute: %w", err)
	}

	rows := newRows(data)
	if err := rows.Preload(); err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *Conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}

	rows, err := c.QueryContext(ctx, "select 1", nil)
	if err != nil {
		return err
	}
	rows.Close()

	return nil
}

func newRows(data []byte) *Rows {
	return &Rows{data: data}
}

type Rows struct {
	data        []byte
	rows        []json.RawMessage
	columnNames []string
	nextIndex   int
}

func (r *Rows) Columns() []string {
	if r.columnNames == nil && len(r.rows) > 0 {
		values, err := UnmarshalRowValues(r.rows[0])
		if err != nil {
			return r.columnNames
		}

		r.columnNames = make([]string, 0, len(values))
		for _, v := range values {
			r.columnNames = append(r.columnNames, v.Key)
		}
	}

	return r.columnNames
}

func (r *Rows) Close() error {
	return nil
}

func (r *Rows) Preload() error {
	if err := json.Unmarshal(r.data, &r.rows); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	more := len(r.rows) > 0 && len(r.rows) > r.nextIndex
	if !more {
		return io.EOF
	}

	row := r.rows[r.nextIndex]
	rowValues, err := UnmarshalRowValues(row)
	if err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	for i, rv := range rowValues {
		if rv.Value != nil {
			dest[i] = driver.Value(rv.Value)
		} else {
			dest[i] = nil
		}
	}

	r.nextIndex += 1

	return nil
}

func getErrorMsg(bs []byte) (string, error) {
	if !json.Valid(bs) {
		return "unknown error", nil
	}

	obj := make(map[string]string)
	if err := json.Unmarshal(bs, &obj); err != nil {
		return "", fmt.Errorf("unmarshal: %w", err)
	}

	return obj["msg"], nil
}

func newSqlTx(ctx context.Context, conn *Conn) *sqlTx {
	return &sqlTx{
		ctx:  ctx,
		sql:  strings.Builder{},
		conn: conn,
	}
}

type sqlTx struct {
	ctx  context.Context
	sql  strings.Builder
	conn *Conn
}

func (tx *sqlTx) Begin() {
	tx.sql.WriteString("begin;")
	tx.sql.WriteByte('\n')
}

func (tx *sqlTx) AppendQuery(bs []byte) {
	if len(bs) == 0 {
		return
	}

	tx.sql.Write(bs)
	if bs[len(bs)-1] != ';' {
		tx.sql.WriteByte(';')
	}

	tx.sql.WriteByte('\n')
}

func (tx *sqlTx) Commit() error {
	tx.sql.WriteString("commit;")
	query := tx.sql.String()

	tx.conn.activeTx = nil
	_, err := tx.conn.ExecContext(tx.ctx, query, nil)

	return err
}

func (tx sqlTx) Rollback() error {
	tx.conn.activeTx = nil

	return nil
}

type RowValue struct {
	Index int
	Key   string
	Value any
}

func UnmarshalRowValues(bs []byte) ([]RowValue, error) {
	d := json.NewDecoder(bytes.NewReader(bs))
	t, err := d.Token()
	if err != nil {
		return nil, err
	}

	var values []RowValue
	switch {
	case t == json.Delim('{'):
		for d.More() {
			t, err := d.Token()
			if err != nil {
				return nil, err
			}

			var val any
			if err := d.Decode(&val); err != nil {
				return nil, err
			}

			values = append(values, RowValue{
				Key:   t.(string),
				Value: val,
			})
		}
	case t == json.Delim('['):
		var index int
		for d.More() {
			var val any
			if err := d.Decode(&val); err != nil {
				return nil, err
			}

			values = append(values, RowValue{
				Index: index,
				Value: val,
			})

			index += 1
		}
	default:
		var val any
		if err := json.Unmarshal(bs, &val); err != nil {
			return nil, err
		}

		return []RowValue{{Value: val}}, nil
	}

	return values, nil
}

func compileQuery(query string, args []driver.NamedValue) ([]byte, error) {
	bs := toBytes(query)
	if len(args) == 0 {
		return bs, nil
	}

	pos := 0
	dst := make([]byte, 0, len(bs))

	var argIdx int
	var err error
	for pos < len(bs) {
		ind := bytes.IndexByte(bs[pos:], '?')
		if ind == -1 {
			if pos == 0 {
				return bs, nil
			}

			dst = append(dst, bs[pos:]...)
			pos = len(bs)

			continue
		}

		bs2 := bs[pos : pos+ind]
		pos += ind + 1

		if len(bs2) > 0 && bs[len(bs2)-1] == '\\' {
			dst = append(dst, bs2[:len(bs2)-1]...)
			dst = append(dst, '?')

			continue
		}

		dst = append(dst, bs2...)

		ind = len(bs) - pos
		for i, c := range bs[pos:] {
			if c >= '0' && c <= '9' {
				continue
			}

			ind = i
			break
		}

		if ind == 0 {
			if argIdx >= len(args) {
				dst = append(dst, '?')

				continue
			}

			dst, err = appendArg(dst, args[argIdx])
			if err != nil {
				return nil, fmt.Errorf("append arg at %d: %w", argIdx, err)
			}

			argIdx += 1

			continue
		}

		bs2 = bs[pos : pos+ind]
		pos += ind

		n := toString(bs)
		idx, err := strconv.Atoi(n)
		if err != nil || idx >= len(args) {
			dst = append(dst, '?')
			dst = append(dst, n...)

			continue
		}

		dst, err = appendArg(dst, args[idx])
		if err != nil {
			return nil, fmt.Errorf("append arg at %d: %w", idx, err)
		}
	}

	return dst, nil
}

func appendArg(bs []byte, arg driver.NamedValue) ([]byte, error) {
	switch v := arg.Value.(type) {
	case nil:
		bs = append(bs, []byte("NULL")...)
	case bool:
		bs = strconv.AppendBool(bs, v)
	case int:
		bs = strconv.AppendInt(bs, int64(v), 10)
	case int32:
		bs = strconv.AppendInt(bs, int64(v), 10)
	case int64:
		bs = strconv.AppendInt(bs, v, 10)
	case uint:
		bs = strconv.AppendInt(bs, int64(v), 10)
	case uint32:
		bs = strconv.AppendInt(bs, int64(v), 10)
	case uint64:
		bs = strconv.AppendInt(bs, int64(v), 10)
	case float32:
		bs = appendFloat(bs, float64(v), 32)
	case float64:
		bs = appendFloat(bs, v, 64)
	case string:
		bs = append(bs, '\'')
		bs = append(bs, v...)
		bs = append(bs, '\'')
	case time.Time:
		bs = strconv.AppendInt(bs, v.UnixMicro(), 10)
	case []byte:
		bs = append(bs, '\'')
		bs = append(bs, hex.EncodeToString(v)...)
		bs = append(bs, '\'')
	default:
		return nil, errors.New(fmt.Sprintf("unsupported type (%s)", reflect.TypeOf(v)))
	}

	return bs, nil
}

func appendFloat(bs []byte, num float64, bitSize int) []byte {
	switch {
	case math.IsNaN(num):
		return append(bs, "'NaN'"...)
	case math.IsInf(num, 1):
		return append(bs, "'Infinity'"...)
	case math.IsInf(num, -1):
		return append(bs, "'-Infinity'"...)
	default:
		return strconv.AppendFloat(bs, num, 'f', -1, bitSize)
	}
}

func toBytes(s string) []byte {
	if s == "" {
		return []byte{}
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func toString(bs []byte) string {
	if len(bs) == 0 {
		return ""
	}
	return unsafe.String(&bs[0], len(bs))
}

func NewHTTPExecutor(authToken string) *HTTPExecutor {
	return &HTTPExecutor{
		client:    &http.Client{},
		authToken: authToken,
	}
}

type HTTPExecutor struct {
	client    *http.Client
	authToken string
}

func (ex *HTTPExecutor) Execute(ctx context.Context, url string, query []byte) ([]byte, error) {
	req, err := ex.request(ctx, url, query)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}

	res, err := ex.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	defer res.Body.Close()

	bs, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	switch {
	case res.StatusCode == http.StatusOK || res.StatusCode == http.StatusNoContent:
		return bs, nil
	case res.StatusCode == http.StatusForbidden:
		return nil, ErrUnauthorized
	default:
		msg, err := getErrorMsg(bs)
		if err != nil {
			return nil, fmt.Errorf("get error msg: %w", err)
		}

		return nil, errors.New(msg)
	}
}

func (ex *HTTPExecutor) request(ctx context.Context, url string, bs []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "text/plain")

	if len(ex.authToken) > 0 {
		req.Header.Set("Authorization", "Bearer "+ex.authToken)
	}

	return req, nil
}

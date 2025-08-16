package native

import (
	_ "embed"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

// https://github.com/amelielabs/amelie/commit/ebaa52b6e248bf95076ba0f5f36302a9d7b62eac
//
//go:embed libs/libamelie.so
var libamelieBytes []byte

var complete = purego.NewCallback(func(ptr unsafe.Pointer) {
	ch := (*chan struct{})(ptr)
	(*ch) <- struct{}{}
})

var (
	amelie_init    func() uintptr
	amelie_free    func(uintptr)
	amelie_open    func(uintptr, string, int32, unsafe.Pointer) int32
	amelie_connect func(uintptr, uintptr) uintptr
	amelie_execute func(uintptr, string, int32, unsafe.Pointer, uintptr, unsafe.Pointer) uintptr
	amelie_wait    func(uintptr, int32, unsafe.Pointer) int32
)

var registerLib = sync.OnceFunc(func() {
	libameliePath := os.Getenv("LIBAMELIE_PATH")
	if len(libameliePath) == 0 {
		path, closer, err := dumpEmbeddedLib()
		if err != nil {
			panic(fmt.Sprintf("dump libamelie: %s", err))
		}
		defer func() {
			if err := closer(); err != nil {
				panic(fmt.Sprintf("error removing %s: %s", path, err))
			}
		}()

		libameliePath = path
	}

	libamelie, err := purego.Dlopen(libameliePath, purego.RTLD_LAZY|purego.RTLD_GLOBAL)
	if err != nil {
		panic(fmt.Sprintf("open libamelie.so: %s", err))
	}

	purego.RegisterLibFunc(&amelie_init, libamelie, "amelie_init")
	purego.RegisterLibFunc(&amelie_free, libamelie, "amelie_free")
	purego.RegisterLibFunc(&amelie_open, libamelie, "amelie_open")
	purego.RegisterLibFunc(&amelie_connect, libamelie, "amelie_connect")
	purego.RegisterLibFunc(&amelie_execute, libamelie, "amelie_execute")
	purego.RegisterLibFunc(&amelie_wait, libamelie, "amelie_wait")
})

func URL(path string, args ...string) string {
	u := new(url.URL)
	u.Scheme = "file"

	parts := strings.Split(path, "/")
	if len(parts) > 0 {
		u.Host = parts[0]
		u.Path = strings.Join(parts[1:], "/")
	}

	vs := make(url.Values)
	last := len(args) - 1
	for i := 0; i < len(args); i += 2 {
		var value string
		if i < last {
			value = args[i+1]
		}
		vs.Set(args[i], value)
	}
	u.RawQuery = vs.Encode()

	return u.String()
}

type amelieArg struct {
	data     uintptr
	dataSize uint64
}

func NewDriver() *Driver {
	registerLib()

	return &Driver{
		amelie: amelie_init(),
	}
}

type Driver struct {
	amelie uintptr
}

func (d *Driver) Open(url *url.URL) int32 {
	path := url.Host + url.Path
	args := url.Query()
	argc := len(args)
	argv := make([]*byte, 0, argc)

	b := strings.Builder{}
	for k, vs := range args {
		if len(vs) == 0 {
			continue
		}
		b.Grow(len(k) + len(vs[0]) + 4)
		b.WriteByte('-')
		b.WriteByte('-')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(vs[0])
		argv = append(argv, cString(b.String()))
		b.Reset()
	}

	return amelie_open(d.amelie, path, int32(argc), unsafe.Pointer(unsafe.SliceData(argv)))
}

func (d *Driver) Connect() *Session {
	return &Session{
		session: amelie_connect(d.amelie, 0),
	}
}

func (d *Driver) Close() {
	amelie_free(d.amelie)
}

type Session struct {
	session uintptr
}

func WaitAll(rs []*RequestResult) {
	var wg sync.WaitGroup
	wg.Add(len(rs))

	for i := range rs {
		go func() {
			defer wg.Done()
			<-rs[i].done
			close(rs[i].done)
		}()
	}

	wg.Wait()
}

type RequestResult struct {
	req  uintptr
	done chan struct{}
}

func (r *RequestResult) Wait() ([]byte, int) {
	defer amelie_free(r.req)
	<-r.done

	result := new(amelieArg)
	rc := amelie_wait(r.req, -1, unsafe.Pointer(result))
	if result.dataSize == 0 {
		return nil, int(rc)
	}

	return goStringBytesSized(result.data, int(result.dataSize)), int(rc)
}

func (s *Session) Execute(query string) *RequestResult {
	done := make(chan struct{})
	req := amelie_execute(s.session, query, 0, nil, complete, unsafe.Pointer(&done))
	return &RequestResult{req, done}
}

func (s *Session) Close() {
	amelie_free(s.session)
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func cString(name string) *byte {
	if hasSuffix(name, "\x00") {
		return &(*(*[]byte)(unsafe.Pointer(&name)))[0]
	}
	bs := make([]byte, len(name)+1)
	copy(bs, name)
	return &bs[0]
}

func goStringBytesSized(c uintptr, size int) []byte {
	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&c))
	if ptr == nil {
		return nil
	}
	bs := make([]byte, size)
	copy(bs, unsafe.Slice((*byte)(ptr), size))
	return bs
}

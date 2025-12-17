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

// amelie 0.8.0
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
	amelie_open    func(uintptr, unsafe.Pointer, int, unsafe.Pointer) int
	amelie_connect func(uintptr, unsafe.Pointer) uintptr
	amelie_execute func(uintptr, unsafe.Pointer, int, unsafe.Pointer, uintptr, unsafe.Pointer) uintptr
	amelie_wait    func(uintptr, int32, unsafe.Pointer) int
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

	libamelie, err := purego.Dlopen(libameliePath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		panic(fmt.Sprintf("open libamelie: %s", err))
	}

	purego.RegisterLibFunc(&amelie_init, libamelie, "amelie_init")
	purego.RegisterLibFunc(&amelie_free, libamelie, "amelie_free")
	purego.RegisterLibFunc(&amelie_open, libamelie, "amelie_open")
	purego.RegisterLibFunc(&amelie_connect, libamelie, "amelie_connect")
	purego.RegisterLibFunc(&amelie_execute, libamelie, "amelie_execute")
	purego.RegisterLibFunc(&amelie_wait, libamelie, "amelie_wait")
})

type amelieArg struct {
	data     uintptr
	dataSize uint
}

func NewDriver(url *url.URL) *Driver {
	registerLib()

	return &Driver{
		amelie: amelie_init(),
		url:    url,
	}
}

type Driver struct {
	amelie uintptr
	url    *url.URL
}

func (d *Driver) Open() int {
	if d.url.Scheme != "amelie" {
		return amelie_open(d.amelie, nil, 0, nil)
	}

	dir := d.url.Host + d.url.Path
	args := d.url.Query()
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

	return amelie_open(d.amelie, unsafe.Pointer(cString(dir)), argc, unsafe.Pointer(unsafe.SliceData(argv)))
}

func (d *Driver) Connect() *Session {
	var uri unsafe.Pointer
	if d.url.Scheme != "amelie" {
		uri = unsafe.Pointer(cString(d.url.String()))
	}

	return &Session{
		session: amelie_connect(d.amelie, uri),
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

	return goStringBytes(result.data, int(result.dataSize)), int(rc)
}

func (s *Session) Execute(query []byte) *RequestResult {
	done := make(chan struct{})
	req := amelie_execute(s.session, unsafe.Pointer(cByteString(query)), 0, nil, complete, unsafe.Pointer(&done))
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

func cByteString(bs []byte) *byte {
	if len(bs) == 0 {
		bs = make([]byte, 1)
		return &bs[0]
	}
	if bs[len(bs)-1] == '\x00' {
		return &bs[0]
	}
	bs2 := make([]byte, len(bs)+1)
	copy(bs2, bs)
	return &bs2[0]
}

func goStringBytes(c uintptr, size int) []byte {
	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&c))
	if ptr == nil {
		return nil
	}
	if size == 0 {
		for {
			if *(*byte)(unsafe.Add(ptr, uintptr(size))) == '\x00' {
				break
			}
			size++
		}
	}
	bs := make([]byte, size)
	copy(bs, unsafe.Slice((*byte)(ptr), size))
	return bs
}

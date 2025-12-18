package native

import (
	_ "embed"
	"fmt"
	"net/url"
	"os"
	"runtime"
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
	amelie_open    func(uintptr, *byte, int, **byte) int
	amelie_connect func(uintptr, *byte) uintptr
	amelie_execute func(uintptr, *byte, int, uintptr, uintptr, *chan struct{}) uintptr
	amelie_wait    func(uintptr, int32, *amelieArg) int
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
		env: amelie_init(),
		url: url,
	}
}

type Driver struct {
	env uintptr
	url *url.URL
}

func (d *Driver) Open() int {
	if d.url.Scheme != "amelie" {
		return amelie_open(d.env, nil, 0, nil)
	}

	argq := d.url.Query()
	argc := len(argq)
	argv := make([]*byte, 0, argc)
	args := make([]any, 0, argc)
	for k, vs := range argq {
		if len(vs) == 0 {
			continue
		}

		bs := make([]byte, 0, len(k)+len(vs[0])+3)
		bs = append(bs, '-', '-')
		bs = append(bs, k...)
		bs = append(bs, '=')
		bs = append(bs, vs[0]...)
		bs = cStringBytes(bs)

		args = append(args, bs)
		argv = append(argv, unsafe.SliceData(bs))
	}

	path := cStringBytes([]byte(d.url.Host + d.url.Path))
	rc := amelie_open(d.env, unsafe.SliceData(path), argc, unsafe.SliceData(argv))

	runtime.KeepAlive(args)

	return rc
}

func (d *Driver) Connect() *Session {
	var session uintptr
	if d.url.Scheme == "amelie" {
		session = amelie_connect(d.env, nil)
	} else {
		url := cStringBytes([]byte(d.url.String()))
		session = amelie_connect(d.env, unsafe.SliceData(url))
	}

	return &Session{
		session: session,
	}
}

func (d *Driver) Close() {
	amelie_free(d.env)
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
	rc := amelie_wait(r.req, -1, result)
	if result.dataSize == 0 {
		return nil, int(rc)
	}

	return goStringBytes(result.data, int(result.dataSize)), int(rc)
}

func (s *Session) Execute(query []byte) *RequestResult {
	done := make(chan struct{})
	query = cStringBytes(query)
	req := amelie_execute(s.session, unsafe.SliceData(query), 0, 0, complete, &done)
	return &RequestResult{req, done}
}

func (s *Session) Close() {
	amelie_free(s.session)
}

func cStringBytes(bs []byte) []byte {
	if len(bs) == 0 {
		bs = make([]byte, 1)
		return bs
	}
	if bs[len(bs)-1] == '\x00' {
		return bs
	}
	bs2 := make([]byte, len(bs)+1)
	copy(bs2, bs)
	return bs2
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

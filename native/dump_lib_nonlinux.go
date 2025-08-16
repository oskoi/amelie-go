//go:build !linux

package native

func dumpEmbeddedLib() (path string, closer func() error, err error) {
	panic("unsupported os (only linux)")
}

package native

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func dumpEmbeddedLib() (path string, closer func() error, err error) {
	fd, err := unix.MemfdCreate("libamelia", 0)
	if err != nil {
		return "", nil, fmt.Errorf("error creating memfd: %w", err)
	}

	file := os.NewFile(uintptr(fd), fmt.Sprintf("/proc/self/fd/%d", fd))
	if file == nil {
		return "", nil, errors.New("error creating file from fd")
	}

	defer func() {
		if file != nil && err != nil {
			if closeErr := file.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("error closing file: %w", closeErr))
			}
		}
	}()

	if _, err := io.Copy(file, bytes.NewReader(libamelieBytes)); err != nil {
		return "", nil, fmt.Errorf("error copying libamelie to memfd: %w", err)
	}

	return file.Name(), file.Close, nil
}

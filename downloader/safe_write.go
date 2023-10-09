package downloader

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

type SafeWriteConfig struct {
	TempDir       string
	DirMask       fs.FileMode
	ForceDirMask  bool
	ForceFileMask bool
}

func (c SafeWriteConfig) RemoveTemp(storage afero.Fs) error {
	return storage.RemoveAll(c.TempDir)
}

func (c SafeWriteConfig) SafeWrite(
	storage afero.Fs,
	dest string,
	filePerm fs.FileMode,
	r io.Reader,
	postProcess ...(func(filename string, written int64) error),
) (err error) {
	dest = filepath.FromSlash(dest)
	dest = filepath.Clean(dest)

	if strings.HasPrefix(dest, "..") {
		return fmt.Errorf("dest must not begin with .. dest = %s", dest)
	}

	tempDest := filepath.Join(c.TempDir, dest)
	if strings.HasPrefix(tempDest, "..") {
		return fmt.Errorf("dest must not begin with .. dest = %s", dest)
	}

	mask := c.DirMask & fs.ModePerm
	if mask == 0 {
		mask = 0o755
	}

	for _, filename := range []string{dest, tempDest} {
		dir := filepath.Dir(filename)
		err = storage.MkdirAll(dir, mask)
		if err != nil {
			return err
		}

		if c.ForceDirMask {
			dir2 := dir
			for dir2 != "" && dir2 != "." && dir2 != string(os.PathSeparator) {
				_ = storage.Chmod(dir2, mask)
				dir2 = filepath.Dir(dir2)
			}
		}
	}

	filePerm = filePerm & fs.ModePerm
	if filePerm == 0 {
		filePerm = 0o666
	}
	f, err := storage.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = storage.Remove(dest)
		}
	}()

	n, err := io.Copy(f, r)
	if err != nil {
		return err
	}

	for _, p := range postProcess {
		if err := p(dest, n); err != nil {
			return err
		}
	}

	err = storage.Rename(tempDest, dest)
	if err != nil {
		return err
	}

	return nil
}

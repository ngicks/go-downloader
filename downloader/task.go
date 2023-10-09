package downloader

import (
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/spf13/afero"
)

type TaskState struct {
	mu sync.Mutex

	State             State
	Downloaded        *atomic.Int64
	Task              Task
	DownloadTaskState []*DownloadTaskState
	Err               error
}

func NewTaskState(t Task) *TaskState {
	downloadTaskState := make([]*DownloadTaskState, len(t.Tasks))
	for i, task := range t.Tasks {
		if task.Dest == "" {
			task.Dest = fmt.Sprintf("%s_%03d", t.Dest, i)
		}
		downloadTaskState[i] = NewDownloadTaskState(task)
	}

	return &TaskState{
		State:             StateWaiting,
		Downloaded:        &atomic.Int64{},
		DownloadTaskState: downloadTaskState,
		Task:              t,
	}
}

type StaticTaskState struct {
	State             State
	Downloaded        int64
	Task              Task
	DownloadTaskState []StaticDownloadTaskState
	Err               error
}

func (t *TaskState) Snapshot() StaticTaskState {
	t.mu.Lock()
	defer t.mu.Unlock()

	downloadTaskState := make([]StaticDownloadTaskState, len(t.DownloadTaskState))
	for i, state := range t.DownloadTaskState {
		downloadTaskState[i] = state.Snapshot()
	}

	return StaticTaskState{
		State:             t.State,
		Downloaded:        t.Downloaded.Load(),
		Task:              t.Task,
		DownloadTaskState: downloadTaskState,
		Err:               t.Err,
	}
}

type DownloadTaskState struct {
	mu sync.Mutex

	State        State
	Downloaded   *atomic.Int64
	DownloadTask DownloadTask
	Err          error
}

func NewDownloadTaskState(t DownloadTask) *DownloadTaskState {
	return &DownloadTaskState{
		State:        StateWaiting,
		Downloaded:   new(atomic.Int64),
		DownloadTask: t,
	}
}

func (s *DownloadTaskState) update(fn func(s *DownloadTaskState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s)
}

type StaticDownloadTaskState struct {
	State        State
	Downloaded   int64
	DownloadTask DownloadTask
	Err          error
}

func (s *DownloadTaskState) Snapshot() StaticDownloadTaskState {
	return StaticDownloadTaskState{
		State:        s.State,
		Downloaded:   s.Downloaded.Load(),
		DownloadTask: s.DownloadTask,
		Err:          s.Err,
	}
}

type State string

const (
	StateWaiting       State = "waiting"
	StateDownloading   State = "downloading"
	StateConcatenating State = "concatenating"
	StateDone          State = "done"
	StateErr           State = "err"
)

type Task struct {
	// Download destination path. Dest should be slash delimited.
	Dest            string
	ArchiveAlgo     ArchiverAlgo
	CompressionAlgo CompressionAlgo
	Validity        Validity
	Tasks           []DownloadTask
}

type DownloadTask struct {
	// Download destination path. Dest should be slash delimited.
	Dest string
	// Target url
	Url      *url.URL
	Validity Validity
}

type Validity struct {
	Size             int
	Checksum         string
	ChecksumEncoding ChecksumEncoding
	ChecksumAlgo     crypto.Hash
}

func (v Validity) Hash() (h hash.Hash, ok bool) {
	if v.ChecksumAlgo != 0 {
		return v.ChecksumAlgo.New(), true
	}
	return nil, false
}

var ErrCheckSumUnmatched = errors.New("check sum unmatched")

func (v Validity) Validate(b []byte) error {
	encoded := v.ChecksumEncoding.Encode(b)
	if encoded != v.Checksum {
		return fmt.Errorf("%w, expected = %s, actually = %s", v.Checksum, encoded)
	}
	return nil
}

type CompressionAlgo string

const (
	Bzip2 CompressionAlgo = "bzip2"
	Flate CompressionAlgo = "flate"
	Gzip  CompressionAlgo = "gzip"
	// Lzw   CompressionAlgo = "lzw"
	Zlib CompressionAlgo = "zlib"
)

func (a CompressionAlgo) Decompress(r io.Reader) (io.ReadCloser, error) {
	switch a {
	case Bzip2:
		return io.NopCloser(bzip2.NewReader(r)), nil
	case Flate:
		return flate.NewReader(r), nil
	case Gzip:
		return gzip.NewReader(r)
	case Zlib:
		return zlib.NewReader(r)
	default:
		return nil, fmt.Errorf("CompressionAlgo: unknown compression algo")
	}
}

type ArchiverAlgo string

const (
	Tar ArchiverAlgo = "tar"
	// Zip ArchiverAlgo = "zip"
)

func (a ArchiverAlgo) Unarchive(ctx context.Context, storage afero.Fs, r io.Reader) error {
	switch a {
	case Tar:
		return Untar(ctx, storage, r, 0o777)
	default:
		return fmt.Errorf("ArchiverAlgo: unknown archiver algo")
	}
}

type ChecksumEncoding string

const (
	Base64 ChecksumEncoding = "base64"
	Hex    ChecksumEncoding = "hex"
)

func (e ChecksumEncoding) Encode(data []byte) string {
	switch e {
	case Base64:
		return base64.StdEncoding.EncodeToString(data)
	default:
		return hex.EncodeToString(data)
	}
}

func (e ChecksumEncoding) Decode(data string) ([]byte, error) {
	switch e {
	case Base64:
		return base64.StdEncoding.DecodeString(data)
	default:
		return hex.DecodeString(data)
	}
}

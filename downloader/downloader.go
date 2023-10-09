package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/ngicks/eventqueue"
	"github.com/spf13/afero"
)

type Requester interface {
	Request(ctx context.Context, target *url.URL) (io.ReadCloser, error)
}

type RequesterFunc func(ctx context.Context, target *url.URL) (io.ReadCloser, error)

func (f RequesterFunc) Request(ctx context.Context, target *url.URL) (io.ReadCloser, error) {
	return f(ctx, target)
}

type DownloaderWorker struct {
	runMu           sync.Mutex
	storage         afero.Fs
	safeWriteConfig SafeWriteConfig
	requester       Requester
	queue           *eventqueue.EventQueue[*DownloadTaskState]
	taskChan        <-chan *DownloadTaskState
	limit           uint
}

func NewDownloaderWorker(
	storage afero.Fs,
	safeWriteConfig SafeWriteConfig,
	limit uint,
	requester Requester,
) *DownloaderWorker {
	if limit == 0 {
		limit = 1
	}

	if requester == nil {
		requester = RequesterFunc(func(ctx context.Context, target *url.URL) (io.ReadCloser, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
			if err != nil {
				return nil, err
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}
			if res.StatusCode < 200 || 300 <= res.StatusCode {
				_ = res.Body.Close()
				return nil, fmt.Errorf(
					"response status code is not 2xx. target url = %s, code = %d",
					target.String(), res.StatusCode,
				)
			}
			return res.Body, nil
		})
	}

	sink := eventqueue.NewChannelSink[*DownloadTaskState](0)
	q := eventqueue.New(sink)

	return &DownloaderWorker{
		storage:         storage,
		safeWriteConfig: safeWriteConfig,
		requester:       requester,
		queue:           q,
		taskChan:        sink.Outlet(),
		limit:           limit,
	}
}

func (w *DownloaderWorker) Send(t *DownloadTaskState) {
	w.queue.Push(t)
}

func (w *DownloaderWorker) Run(ctx context.Context) {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = w.queue.Run(ctx)
	}()

	for i := 0; i < int(w.limit); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.run(ctx)
		}()
	}
	wg.Wait()
}

func (w *DownloaderWorker) run(ctx context.Context) {
LOOP:
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-w.taskChan:
			data, err := w.requester.Request(ctx, t.DownloadTask.Url)
			if err != nil {
				t.update(func(s *DownloadTaskState) {
					s.Err = err
				})
				continue LOOP
			} else {
				t.update(func(s *DownloadTaskState) {
					s.State = StateDownloading
				})
			}

			r := data
			var validating *validating
			if h, ok := t.DownloadTask.Validity.Hash(); ok {
				validating = newValidating(r, h)
				r = validating
			}
			r = newProgressUpdating(r, t.Downloaded)

			err = w.safeWriteConfig.SafeWrite(
				w.storage,
				t.DownloadTask.Dest,
				0o666,
				r,
				func(filename string, written int64) error {
					if written != int64(t.DownloadTask.Validity.Size) {
						return fmt.Errorf(
							"short write. target url = %s, dest = %s",
							t.DownloadTask.Url.String(), t.DownloadTask.Dest,
						)
					}
					return nil
				},
				func(filename string, written int64) error {
					if validating != nil {
						sum := validating.HashSum(nil)
						if err := t.DownloadTask.Validity.Validate(sum); err != nil {
							return err
						}
					}
					return nil
				},
			)

			_ = data.Close()

			if err != nil {
				t.update(func(s *DownloadTaskState) {
					s.Err = err
				})
			} else {
				t.update(func(s *DownloadTaskState) {
					s.State = StateDone
				})
			}
		}
	}
}

// type Downloader struct {
// 	mu      sync.Mutex
// 	storage afero.Fs
// 	current *orderedmap.Pair[string, TaskState]
// 	tasks   *orderedmap.OrderedMap[string, TaskState]

// 	notification chan struct{}
// 	sem          *semaphore.Weighted

// 	clientFactory ClientFactory
// }

// func New(storage afero.Fs, opts ...Option) *Downloader {
// 	d := &Downloader{
// 		storage:      storage,
// 		tasks:        orderedmap.New[string, TaskState](),
// 		notification: make(chan struct{}, 1),
// 		sem:          semaphore.NewWeighted(5),
// 		clientFactory: ClientFactoryFunc(
// 			func(target *url.URL) (*http.Client, error) {
// 				return http.DefaultClient, nil
// 			},
// 		),
// 	}

// 	for _, opt := range opts {
// 		opt(d)
// 	}

// 	return d
// }

// func (d *Downloader) AddTask(t Task) {
// 	d.mu.Lock()
// 	defer d.mu.Unlock()
// 	d.tasks.Store(t.Dest, TaskState{State: StateWaiting, Task: t})
// 	select {
// 	case d.notification <- struct{}{}:
// 	default:
// 	}
// }

// func (d *Downloader) Run(ctx context.Context) error {
// 	defer func() {
// 		select {
// 		case <-d.notification:
// 		default:
// 		}
// 	}()
// 	for {
// 		err := d.doTasks(ctx)
// 		if err != nil {
// 			return err
// 		}

// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		case <-d.notification:
// 		}
// 	}
// }

// func (d *Downloader) doTasks(ctx context.Context) (err error) {
// 	if d.tasks.Len() == 0 {
// 		return nil
// 	}

// 	d.mu.Lock()
// 	if d.current == nil {
// 		d.current = d.tasks.Oldest()
// 	}
// 	d.mu.Unlock()

// 	for p := d.current; p != nil; p = p.Next() {
// 		if p.Value.State != StateWaiting {
// 			continue
// 		}
// 		d.mu.Lock()
// 		d.current = p
// 		d.mu.Unlock()

// 	}
// }

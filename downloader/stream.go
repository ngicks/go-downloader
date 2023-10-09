package downloader

import (
	"context"
	"hash"
	"io"
	"sync/atomic"
)

var _ io.ReadCloser = (*cancellableStream)(nil)

type cancellableStream struct {
	ctx context.Context
	r   io.Reader
}

func newCancellableStream(ctx context.Context, r io.ReadCloser) *cancellableStream {
	return &cancellableStream{
		ctx: ctx,
		r:   r,
	}
}

func (c *cancellableStream) Read(p []byte) (n int, err error) {
	if c.ctx.Err() != nil {
		return 0, c.ctx.Err()
	}
	return c.r.Read(p)
}

func (c *cancellableStream) Close() error {
	if closer, ok := c.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type validating struct {
	r   io.Reader
	tee io.Reader
	h   hash.Hash
}

func newValidating(r io.Reader, h hash.Hash) *validating {
	tee := io.TeeReader(r, h)
	return &validating{
		r:   r,
		tee: tee,
		h:   h,
	}
}

func (v *validating) Read(p []byte) (n int, err error) {
	return v.tee.Read(p)
}

func (c *validating) Close() error {
	if closer, ok := c.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (c *validating) HashSum(b []byte) []byte {
	return c.h.Sum(b)
}

var _ io.ReadCloser = (*progressUpdating)(nil)

type progressUpdating struct {
	r    io.Reader
	size *atomic.Int64
}

func newProgressUpdating(r io.Reader, size *atomic.Int64) *progressUpdating {
	return &progressUpdating{
		r:    r,
		size: size,
	}
}

func (r *progressUpdating) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.size.Add(int64(n))
	return n, err
}

func (r *progressUpdating) Close() error {
	if closer, ok := r.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

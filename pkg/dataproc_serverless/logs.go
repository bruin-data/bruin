package dataprocserverless

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type LogLine struct {
	Message string
	Source  string
}

// GCSLogConsumer streams the Spark driver output that Dataproc Serverless writes
// to the batch's staging bucket (RuntimeInfo.OutputUri). This is the same source
// that `gcloud dataproc batches wait` streams.
//
// We deliberately do not read job logs from Cloud Logging: Dataproc Serverless
// writes the driver/executor stdout, stderr and log4j output to this GCS
// location, and that output does not reliably appear in Cloud Logging (only
// control-plane logs such as the autoscaler do).
type GCSLogConsumer struct {
	ctx           context.Context //nolint:containedctx
	storageClient *storage.Client

	bucket   string
	prefix   string
	resolved bool

	// offsets tracks, per output object, how many bytes have already been read.
	offsets map[string]int64
	// partial holds bytes that have been read but not yet terminated by a
	// newline, per output object. They are completed on a subsequent read or
	// emitted as-is on Flush.
	partial map[string][]byte
}

func newGCSLogConsumer(ctx context.Context, client *storage.Client) *GCSLogConsumer {
	return &GCSLogConsumer{
		ctx:           ctx,
		storageClient: client,
		offsets:       map[string]int64{},
		partial:       map[string][]byte{},
	}
}

// SetOutputURI records the GCS location of the driver output. Dataproc only
// populates RuntimeInfo.OutputUri once the batch starts running, so this is
// called on every poll until a non-empty URI is observed.
func (l *GCSLogConsumer) SetOutputURI(uri string) {
	if uri == "" || l.resolved {
		return
	}
	u, err := url.Parse(uri)
	if err != nil {
		return
	}
	l.bucket = u.Host
	l.prefix = strings.TrimPrefix(u.Path, "/")
	l.resolved = true
}

// Next returns any complete log lines written since the last call.
func (l *GCSLogConsumer) Next() []LogLine {
	return l.read(false)
}

// Flush returns any remaining buffered output, including a trailing line that is
// not newline-terminated. It should be called once after the batch reaches a
// terminal state.
func (l *GCSLogConsumer) Flush() []LogLine {
	return l.read(true)
}

func (l *GCSLogConsumer) read(flush bool) []LogLine {
	if !l.resolved {
		return nil
	}

	bucket := l.storageClient.Bucket(l.bucket)

	// Driver output is split across objects named "<prefix>.000000000",
	// "<prefix>.000000001", ... List them and pull any new bytes into the
	// per-object buffers. The listing already reports each object's size, so we
	// avoid a second Attrs call per object on every poll.
	//
	// Listing is best-effort: if it fails (e.g. the context was cancelled), we
	// still fall through to drainBuffers so that any bytes already buffered are
	// emitted — in particular a final Flush must never drop a buffered partial
	// line just because the listing failed.
	it := bucket.Objects(l.ctx, &storage.Query{Prefix: l.prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			break
		}
		l.fetch(bucket, attrs.Name, attrs.Size)
	}

	return l.drainBuffers(flush)
}

// fetch reads the bytes appended to an output object since the last read into
// the per-object buffer, advancing the object's offset.
func (l *GCSLogConsumer) fetch(bucket *storage.BucketHandle, name string, size int64) {
	off := l.offsets[name]
	if size <= off {
		return
	}

	reader, err := bucket.Object(name).NewRangeReader(l.ctx, off, -1)
	if err != nil {
		return
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return
	}

	l.offsets[name] = off + int64(len(data))
	l.partial[name] = append(l.partial[name], data...)
}

// drainBuffers emits complete lines from every buffered object in lexical order
// (which matches the order Dataproc writes them). It does not touch GCS, so it
// always flushes whatever is buffered regardless of listing failures.
func (l *GCSLogConsumer) drainBuffers(flush bool) []LogLine {
	names := make([]string, 0, len(l.partial))
	for name := range l.partial {
		names = append(names, name)
	}
	sort.Strings(names)

	lines := []LogLine{}
	for _, name := range names {
		complete, rest := splitLines(l.partial[name], flush)
		if len(rest) == 0 {
			delete(l.partial, name)
		} else {
			l.partial[name] = rest
		}
		for _, msg := range complete {
			lines = append(lines, LogLine{Source: "DRIVER", Message: msg})
		}
	}

	return lines
}

// splitLines splits buffered bytes into complete (newline-terminated) lines.
// When flush is false, bytes after the final newline are returned as rest so
// they can be completed on a later read. When flush is true, any trailing bytes
// are emitted as a final line.
func splitLines(buf []byte, flush bool) (lines []string, rest []byte) {
	for {
		idx := bytes.IndexByte(buf, '\n')
		if idx < 0 {
			break
		}
		lines = append(lines, strings.TrimRight(string(buf[:idx]), "\r"))
		buf = buf[idx+1:]
	}
	if flush && len(buf) > 0 {
		lines = append(lines, strings.TrimRight(string(buf), "\r"))
		buf = nil
	}
	return lines, buf
}

// NoOpLogConsumer is a log consumer that does nothing.
type NoOpLogConsumer struct{}

func (l *NoOpLogConsumer) Next() []LogLine { return nil }

func (l *NoOpLogConsumer) Flush() []LogLine { return nil }

func (l *NoOpLogConsumer) SetOutputURI(string) {}

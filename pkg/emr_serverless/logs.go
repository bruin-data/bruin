package emr_serverless //nolint

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type LogLine struct {
	Stream  string
	Message string
}

type logState struct {
	size int64
	read int
}

type S3LogConsumer struct {
	Ctx   context.Context
	S3cli *s3.Client
	URI   *url.URL
	RunID string
	AppID string

	once  sync.Once
	state map[string]logState
}

func (l *S3LogConsumer) Next() (lines []LogLine) {
	l.once.Do(func() {
		l.state = make(map[string]logState)
	})

	lines = append(lines, l.readStream("stdout")...)
	lines = append(lines, l.readStream("stderr")...)

	return lines
}

func (l *S3LogConsumer) readStream(stream string) (lines []LogLine) {

	if !l.hasNewLogs(stream) {
		return nil
	}

	logStream, err := l.S3cli.GetObject(l.Ctx, &s3.GetObjectInput{
		Bucket: &l.URI.Host,
		Key:    aws.String(l.logFile(stream)),
	})

	if err != nil {
		return nil
	}
	defer logStream.Body.Close()

	gzReader, err := gzip.NewReader(logStream.Body)
	if err != nil {
		return nil
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	for range l.state[stream].read {
		scanner.Scan()
	}

	for scanner.Scan() {
		lines = append(lines, LogLine{
			Stream:  stream,
			Message: strings.TrimSpace(scanner.Text()),
		})
	}
	l.state[stream] = logState{
		read: len(lines) + l.state[stream].read,
		size: *logStream.ContentLength,
	}
	return
}

func (l *S3LogConsumer) logFile(stream string) string {
	fullPath := l.URI.JoinPath(
		"applications",
		l.AppID,
		"jobs",
		l.RunID,
		"SPARK_DRIVER",
		fmt.Sprintf("%s.gz", stream),
	).Path

	return strings.TrimPrefix(fullPath, "/")
}

func (l *S3LogConsumer) hasNewLogs(stream string) bool {
	state, exists := l.state[stream]
	if !exists {
		return true
	}

	meta, err := l.S3cli.HeadObject(l.Ctx, &s3.HeadObjectInput{
		Bucket: &l.URI.Host,
		Key:    aws.String(l.logFile(stream)),
	})
	if err != nil {
		return false
	}
	return *meta.ContentLength != state.size
}

type NoOpLogConsumer struct{}

func (l *NoOpLogConsumer) Next() []LogLine {
	return nil
}

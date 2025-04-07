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
	Stream     string
	Message    string
	WorkerType string
}

type logState struct {
	size int64
	read int
}

type S3LogConsumer struct {
	Ctx   context.Context //nolint
	S3cli *s3.Client
	URI   *url.URL
	RunID string
	AppID string

	once  sync.Once
	state map[string]logState
}

type logSource struct {
	URI    *url.URL
	Name   string
	Stream string
}

func (l *S3LogConsumer) Next() (lines []LogLine) { //nolint
	l.once.Do(func() {
		l.state = make(map[string]logState)
	})

	for _, source := range l.listLogSources() {
		lines = append(lines, l.readLogs(source)...)
	}

	return lines
}

func (l *S3LogConsumer) listLogSources() (sources []logSource) {
	jobPath := l.URI.JoinPath(
		"applications",
		l.AppID,
		"jobs",
		l.RunID,
	)
	streams := []string{"stdout", "stderr"}
	for _, stream := range streams {
		sources = append(sources, logSource{
			Name:   "SPARK_DRIVER",
			Stream: stream,
			URI:    jobPath.JoinPath("SPARK_DRIVER", stream+".gz"),
		})
	}

	executorLogsBasePath := jobPath.JoinPath("SPARK_EXECUTOR/")
	objs, err := l.S3cli.ListObjectsV2(l.Ctx, &s3.ListObjectsV2Input{
		Bucket:    &l.URI.Host,
		Prefix:    aws.String(strings.TrimPrefix(executorLogsBasePath.Path, "/")),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return sources
	}
	for _, commonPrefix := range objs.CommonPrefixes {
		prefix := strings.Trim(*commonPrefix.Prefix, "/")
		prefixSegments := strings.Split(prefix, "/")
		id := prefixSegments[len(prefixSegments)-1]
		for _, stream := range streams {
			sources = append(sources, logSource{
				Name:   fmt.Sprintf("SPARK_EXECUTOR(%s)", id),
				Stream: stream,
				URI:    jobPath.JoinPath("SPARK_EXECUTOR", id, stream+".gz"),
			})
		}
	}

	return sources
}

func (l *S3LogConsumer) readLogs(source logSource) []LogLine {
	logStream, err := l.S3cli.GetObject(l.Ctx, &s3.GetObjectInput{
		Bucket: &source.URI.Host,
		Key:    aws.String(strings.TrimPrefix(source.URI.Path, "/")),
	})
	if err != nil {
		return nil
	}
	defer logStream.Body.Close()

	stateKey := fmt.Sprintf("%s:%s", source.Name, source.Stream)
	state, exists := l.state[stateKey]
	if exists && *logStream.ContentLength == state.size {
		return nil
	}

	gzReader, err := gzip.NewReader(logStream.Body)
	if err != nil {
		return nil
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	for range l.state[stateKey].read {
		scanner.Scan()
	}

	lines := []LogLine{}
	for scanner.Scan() {
		lines = append(lines, LogLine{
			WorkerType: source.Name,
			Stream:     source.Stream,
			Message:    strings.TrimSpace(scanner.Text()),
		})
	}
	l.state[stateKey] = logState{
		read: len(lines) + l.state[stateKey].read,
		size: *logStream.ContentLength,
	}
	return lines
}

type NoOpLogConsumer struct{}

func (l *NoOpLogConsumer) Next() []LogLine {
	return nil
}

package emr_serverless //nolint

import (
	"net/url"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type LogLine struct {
	Stream  string
	Message string
}

type S3LogConsumer struct {
	s3cli s3.Client
	uri   *url.URL
}

func (l S3LogConsumer) Next() []LogLine {
	return nil
}

type NoOpLogConsumer struct{}

func (l NoOpLogConsumer) Next() []LogLine {
	return nil
}

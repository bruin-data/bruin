package s3

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// Metadata constraints are optional and apply to each candidate object.
type metadataFilter struct {
	etag          string
	minSize       int64
	modifiedAfter time.Time
}

func parseMetadataFilter(params pipeline.ParameterMap) (metadataFilter, error) {
	f := metadataFilter{}
	if value, ok := params["etag"]; ok {
		etag, valid := value.(string)
		if !valid || strings.Trim(etag, "\"") == "" {
			return f, errors.New("etag must be a non-empty string")
		}
		f.etag = strings.Trim(etag, "\"")
	}
	if _, ok := params["min_size"]; ok {
		value, _ := params.GetString("min_size")
		size, err := strconv.ParseInt(value, 10, 64)
		if err != nil || size < 0 {
			return f, errors.New("min_size must be a non-negative integer in bytes")
		}
		f.minSize = size
	}
	if timestamp, ok := params["last_modified_after"].(time.Time); ok {
		f.modifiedAfter = timestamp
	} else if _, ok := params["last_modified_after"]; ok {
		value, _ := params.GetString("last_modified_after")
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return f, errors.New("last_modified_after must be an RFC3339 timestamp")
		}
		f.modifiedAfter = parsed
	}
	return f, nil
}

func (f metadataFilter) match(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	obj, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		var httpErr *smithyhttp.ResponseError
		if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound {
			return false, nil
		}
		return false, errors.Wrap(err, "failed to check object existence")
	}
	// Strip HTTP quoting only. ETags are opaque, not content hashes.
	if f.etag != "" && (obj.ETag == nil || strings.Trim(*obj.ETag, "\"") != f.etag) {
		return false, nil
	}
	if f.minSize > 0 && (obj.ContentLength == nil || *obj.ContentLength < f.minSize) {
		return false, nil
	}
	return f.modifiedAfter.IsZero() || (obj.LastModified != nil && obj.LastModified.After(f.modifiedAfter)), nil
}

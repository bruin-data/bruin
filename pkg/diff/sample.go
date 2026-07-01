package diff

import "context"

type sampleSizeContextKey struct{}

// WithSampleSize stores the maximum number of records a summarizer should read
// when producing a table summary. A value <= 0 means "scan everything".
//
// Only summarizers that support sampling (currently MongoDB) honor this; the
// SQL-backed summarizers ignore it and always scan the full table.
func WithSampleSize(ctx context.Context, size int64) context.Context {
	return context.WithValue(ctx, sampleSizeContextKey{}, size)
}

// SampleSizeFromContext returns the sample size set via WithSampleSize, or 0 if
// none was set, in which case the whole table should be scanned.
func SampleSizeFromContext(ctx context.Context) int64 {
	if size, ok := ctx.Value(sampleSizeContextKey{}).(int64); ok {
		return size
	}
	return 0
}

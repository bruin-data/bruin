package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

func containsWildcard(key string) bool {
	return strings.ContainsAny(key, "*{")
}

func extractPrefix(key string) string {
	minIdx := len(key)
	for _, ch := range []byte{'*', '{'} {
		if idx := strings.IndexByte(key, ch); idx >= 0 && idx < minIdx {
			minIdx = idx
		}
	}
	prefix := key[:minIdx]
	// Trim back to the last '/' to get a clean prefix boundary
	if lastSlash := strings.LastIndex(prefix, "/"); lastSlash >= 0 {
		return prefix[:lastSlash+1]
	}
	return prefix
}

// Supported patterns:
//   - * matches any characters except /
//   - {a,b,c} matches any of the comma-separated alternatives
func wildcardToRegex(pattern string) string {
	var b strings.Builder
	b.WriteString("^")
	i := 0
	for i < len(pattern) {
		ch := pattern[i]
		switch ch {
		case '*':
			b.WriteString("[^/]*")
			i++
		case '{':
			end := strings.IndexByte(pattern[i:], '}')
			if end < 0 {
				b.WriteString(regexp.QuoteMeta(string(ch)))
				i++
				continue
			}
			alternatives := pattern[i+1 : i+end]
			parts := strings.Split(alternatives, ",")
			b.WriteString("(")
			for j, part := range parts {
				if j > 0 {
					b.WriteString("|")
				}
				part = strings.TrimSpace(part)
				for _, c := range part {
					if c == '*' {
						b.WriteString("[^/]*")
					} else {
						b.WriteString(regexp.QuoteMeta(string(c)))
					}
				}
			}
			b.WriteString(")")
			i += end + 1
		default:
			b.WriteString(regexp.QuoteMeta(string(ch)))
			i++
		}
	}
	b.WriteString("$")
	return b.String()
}

func matchWildcard(ctx context.Context, client *s3.Client, bucket, key string) (bool, error) {
	prefix := extractPrefix(key)
	re, err := regexp.Compile(wildcardToRegex(key))
	if err != nil {
		return false, errors.Wrap(err, "failed to compile wildcard pattern")
	}

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return false, errors.Wrap(err, "failed to list objects")
		}
		for _, obj := range page.Contents {
			if obj.Key != nil && re.MatchString(*obj.Key) {
				return true, nil
			}
		}
	}

	return false, nil
}

type KeySensor struct {
	connection config.ConnectionAndDetailsGetter
	sensorMode string
}

func NewKeySensor(conn config.ConnectionAndDetailsGetter, sensorMode string) *KeySensor {
	return &KeySensor{
		connection: conn,
		sensorMode: sensorMode,
	}
}

func (ks *KeySensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return ks.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (ks *KeySensor) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	if ks.sensorMode == "skip" {
		return nil
	}

	bucketName, ok := t.Parameters["bucket_name"]
	if !ok {
		return errors.New("S3 key sensor requires a parameter named 'bucket_name'")
	}

	bucketKey, ok := t.Parameters["bucket_key"]
	if !ok {
		return errors.New("S3 key sensor requires a parameter named 'bucket_key'")
	}

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	connDetails := ks.connection.GetConnectionDetails(connName)
	if connDetails == nil {
		return errors.Errorf("connection '%s' does not exist", connName)
	}

	var secretKey, accessKey, region, endpointURL string

	awsConn, ok := connDetails.(*config.AwsConnection)
	if ok {
		secretKey = awsConn.SecretKey
		accessKey = awsConn.AccessKey
		region = awsConn.Region
	} else {
		s3Conn, ok2 := connDetails.(*config.S3Connection)
		if !ok2 {
			return errors.Errorf("'%s' either does not exist or is not an AWS/S3 connection", connName)
		}
		secretKey = s3Conn.SecretAccessKey
		accessKey = s3Conn.AccessKeyID
		endpointURL = s3Conn.EndpointURL
	}

	// Load base config without forcing region
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey, secretKey, "",
		)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to load AWS config")
	}

	var s3Client *s3.Client
	if endpointURL != "" {
		// For S3-compatible services (MinIO, R2, etc.), use the custom endpoint
		if region == "" {
			region = "us-east-1"
		}
		cfg.Region = region
		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = &endpointURL
			o.UsePathStyle = true
		})
	} else {
		// For AWS S3, discover region if not set
		if region == "" {
			tmpCfg := cfg
			tmpCfg.Region = "us-east-1" // fallback for discovery
			tmpS3 := s3.NewFromConfig(tmpCfg)

			discoveredRegion, err := manager.GetBucketRegion(ctx, tmpS3, bucketName)
			if err != nil {
				return errors.Wrap(err, "failed to determine bucket region")
			}
			region = discoveredRegion
		}

		cfg.Region = region
		s3Client = s3.NewFromConfig(cfg)
	}

	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		fmt.Fprintln(printer, "Poking S3:", bucketName+"/"+bucketKey)
	}

	isWildcard := containsWildcard(bucketKey)

	timeout := time.After(24 * time.Hour)
	for {
		select {
		case <-timeout:
			return errors.New("Sensor timed out after 24 hours")
		default:
			if isWildcard {
				found, err := matchWildcard(ctx, s3Client, bucketName, bucketKey)
				if err != nil {
					return err
				}
				if found {
					return nil
				}

				if ks.sensorMode == "once" || ks.sensorMode == "" {
					return errors.New("Sensor didn't return the expected result")
				}

				pokeInterval := helpers.GetPokeInterval(ctx, t)
				time.Sleep(time.Duration(pokeInterval) * time.Second)
				if printerExists {
					fmt.Fprintln(printer, "Info: No matching objects found, waiting for", pokeInterval, "seconds")
				}
				continue
			}

			_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: &bucketName,
				Key:    &bucketKey,
			})
			if err != nil {
				var httpErr *smithyhttp.ResponseError
				if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() == http.StatusNotFound {
					if ks.sensorMode == "once" || ks.sensorMode == "" {
						return errors.New("Sensor didn't return the expected result")
					}

					pokeInterval := helpers.GetPokeInterval(ctx, t)
					time.Sleep(time.Duration(pokeInterval) * time.Second)
					if printerExists {
						fmt.Fprintln(printer, "Info: Object not found, waiting for", pokeInterval, "seconds")
					}
					continue
				}
				return errors.Wrap(err, "failed to check object existence")
			}

			return nil
		}
	}
}

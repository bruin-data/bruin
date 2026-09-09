package s3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/objectpattern"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

func matchWildcard(ctx context.Context, client *s3.Client, bucket, key string, filter metadataFilter) (bool, error) {
	prefix := objectpattern.ExtractPrefix(key)
	re, err := regexp.Compile(objectpattern.WildcardToRegex(key))
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
				if filter == (metadataFilter{}) {
					return true, nil
				}
				matched, err := filter.match(ctx, client, bucket, *obj.Key)
				if err != nil || matched {
					return matched, err
				}
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

	bucketName, ok := t.Parameters.GetString("bucket_name")
	if !ok {
		return errors.New("S3 key sensor requires a parameter named 'bucket_name'")
	}

	bucketKey, ok := t.Parameters.GetString("bucket_key")
	if !ok {
		return errors.New("S3 key sensor requires a parameter named 'bucket_key'")
	}

	filter, err := parseMetadataFilter(t.Parameters)
	if err != nil {
		return err
	}

	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	connDetails := ks.connection.GetConnectionDetails(connName)
	if connDetails == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	var secretKey, accessKey, region, endpointURL, sessionToken, caBundle, urlStyle string
	var useSSL *bool

	awsConn, ok := connDetails.(*config.AwsConnection)
	if ok {
		secretKey = awsConn.SecretKey
		accessKey = awsConn.AccessKey
		region = awsConn.Region
	} else {
		s3Conn, ok2 := connDetails.(*config.S3Connection)
		if !ok2 {
			return errors.Errorf("connection '%s' is not an AWS/S3 connection", connName)
		}
		secretKey = s3Conn.SecretAccessKey
		accessKey = s3Conn.AccessKeyID
		endpointURL = s3Conn.EndpointURL
		region = s3Conn.Region
		sessionToken = s3Conn.SessionToken
		caBundle = s3Conn.CABundle
		urlStyle = s3Conn.URLStyle
		useSSL = s3Conn.UseSSL
	}

	if urlStyle != "" && urlStyle != "path" && urlStyle != "vhost" {
		return errors.New("url_style must be 'path' or 'vhost'")
	}
	endpointURL = strings.TrimSpace(endpointURL)
	if endpointURL != "" {
		endpoint, parseErr := url.Parse(endpointURL)
		if parseErr != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
			return errors.New("endpoint_url must be an HTTP(S) URL without credentials, query, or fragment")
		}
		if useSSL != nil && *useSSL != (endpoint.Scheme == "https") {
			return errors.New("use_ssl must agree with the endpoint_url scheme")
		}
	} else if useSSL != nil && !*useSSL {
		return errors.New("use_ssl: false requires an explicit HTTP endpoint_url")
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{}
	if accessKey != "" || secretKey != "" || sessionToken != "" {
		if accessKey == "" || secretKey == "" {
			return errors.New("access_key_id and secret_access_key must both be supplied")
		}
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)))
	}
	if caBundle != "" {
		bundle, readErr := os.Open(caBundle)
		if readErr != nil {
			return errors.Wrap(readErr, "failed to open S3 CA bundle")
		}
		defer bundle.Close()
		loadOptions = append(loadOptions, awsconfig.WithCustomCABundle(bundle))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return errors.Wrap(err, "failed to load AWS config")
	}
	if region == "" {
		region = cfg.Region
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
			o.UsePathStyle = urlStyle != "vhost"
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
		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = urlStyle == "path"
		})
	}

	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		if endpointURL != "" {
			style := "vhost"
			if s3Client.Options().UsePathStyle {
				style = "path"
			}
			fmt.Fprintln(printer, "S3 endpoint:", endpointURL, "addressing:", style)
			if strings.HasPrefix(endpointURL, "http://") {
				fmt.Fprintln(printer, "Warning: S3 HTTP endpoint disables TLS; use only for local development")
			}
		}
		fmt.Fprintln(printer, "Poking S3:", bucketName+"/"+bucketKey)
	}

	isWildcard := objectpattern.ContainsWildcard(bucketKey)

	sensorTimeout := helpers.GetSensorTimeout(t)
	ctx, cancel := context.WithTimeout(ctx, sensorTimeout)
	defer cancel()
	for {
		var found bool
		if isWildcard {
			found, err = matchWildcard(ctx, s3Client, bucketName, bucketKey, filter)
		} else {
			found, err = filter.match(ctx, s3Client, bucketName, bucketKey)
		}
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
		if printerExists {
			fmt.Fprintln(printer, "Info: No matching objects found, waiting for", pokeInterval, "seconds")
		}
		timer := time.NewTimer(time.Duration(pokeInterval) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Wrapf(ctx.Err(), "S3 sensor stopped (timeout %s)", sensorTimeout)
		case <-timer.C:
		}
	}
}

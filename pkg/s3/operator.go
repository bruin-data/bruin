package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
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

type KeySensor struct {
	connection config.ConnectionGetter
	sensorMode string
}

func NewKeySensor(conn config.ConnectionGetter, sensorMode string) *KeySensor {
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

	awsConn, ok := ks.connection.GetConnection(connName).(*config.AwsConnection)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not an AWS connection", connName)
	}

	// Load base config without forcing region
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			awsConn.AccessKey, awsConn.SecretKey, "",
		)),
	)
	if err != nil {
		return errors.Wrap(err, "failed to load AWS config")
	}

	// If region is not set, discover it from the bucket
	region := awsConn.Region
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

	// Rebuild config with correct region
	cfg.Region = region
	s3Client := s3.NewFromConfig(cfg)

	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		fmt.Fprintln(printer, "Poking S3:", bucketName+"/"+bucketKey)
	}

	timeout := time.After(24 * time.Hour)
	for {
		select {
		case <-timeout:
			return errors.New("Sensor timed out after 24 hours")
		default:
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

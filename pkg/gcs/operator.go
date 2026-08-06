package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/objectpattern"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type objectStore interface {
	ObjectExists(ctx context.Context, bucket, object string) (bool, error)
	ObjectMatchingPatternExists(ctx context.Context, bucket, pattern string) (bool, error)
	ObjectWithPrefixExists(ctx context.Context, bucket, prefix string) (bool, error)
	Close() error
}

type storageObjectStore struct {
	client *storage.Client
}

func (s *storageObjectStore) ObjectExists(ctx context.Context, bucket, object string) (bool, error) {
	_, err := s.client.Bucket(bucket).Object(object).Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check GCS object existence: %w", err)
	}
	return true, nil
}

func (s *storageObjectStore) ObjectMatchingPatternExists(
	ctx context.Context,
	bucket,
	pattern string,
) (bool, error) {
	re, err := regexp.Compile(objectpattern.WildcardToRegex(pattern))
	if err != nil {
		return false, fmt.Errorf("failed to compile wildcard pattern: %w", err)
	}

	objects := s.client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: objectpattern.ExtractPrefix(pattern)})
	for {
		attrs, err := objects.Next()
		if errors.Is(err, iterator.Done) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to list GCS objects: %w", err)
		}
		if re.MatchString(attrs.Name) {
			return true, nil
		}
	}
}

func (s *storageObjectStore) ObjectWithPrefixExists(
	ctx context.Context,
	bucket,
	prefix string,
) (bool, error) {
	objects := s.client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	_, err := objects.Next()
	if errors.Is(err, iterator.Done) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to list GCS objects: %w", err)
	}
	return true, nil
}

func (s *storageObjectStore) Close() error {
	return s.client.Close()
}

type objectStoreFactory func(ctx context.Context, connectionDetails any) (objectStore, error)

func newObjectStore(ctx context.Context, connectionDetails any) (objectStore, error) {
	clientOptions, err := storageClientOptions(connectionDetails)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(ctx, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	return &storageObjectStore{client: client}, nil
}

func storageClientOptions(connectionDetails any) ([]option.ClientOption, error) {
	var serviceAccountFile, serviceAccountJSON string
	var useApplicationDefaultCredentials bool

	switch connection := connectionDetails.(type) {
	case *config.GCSConnection:
		serviceAccountFile = connection.ServiceAccountFile
		serviceAccountJSON = connection.ServiceAccountJSON
		useApplicationDefaultCredentials = connection.UseApplicationDefaultCredentials
	case *config.GoogleCloudPlatformConnection:
		if connection.GetCredentials() != nil {
			return []option.ClientOption{option.WithCredentials(connection.GetCredentials())}, nil
		}
		if connection.AccessToken != "" {
			tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: connection.AccessToken})
			return []option.ClientOption{option.WithTokenSource(tokenSource)}, nil
		}
		serviceAccountFile = connection.ServiceAccountFile
		serviceAccountJSON = connection.ServiceAccountJSON
		useApplicationDefaultCredentials = connection.UseApplicationDefaultCredentials
	default:
		return nil, errors.New("connection is not a GCS or Google Cloud Platform connection")
	}

	switch {
	case serviceAccountJSON != "":
		return []option.ClientOption{
			option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(serviceAccountJSON)),
		}, nil
	case serviceAccountFile != "":
		return []option.ClientOption{
			option.WithAuthCredentialsFile(option.ServiceAccount, serviceAccountFile),
		}, nil
	case useApplicationDefaultCredentials:
		return nil, nil
	default:
		return nil, errors.New("GCS credentials are required: provide service_account_file, service_account_json, or enable use_application_default_credentials")
	}
}

type ObjectSensor struct {
	connection config.ConnectionAndDetailsGetter
	sensorMode string
	newStore   objectStoreFactory
}

func NewObjectSensor(conn config.ConnectionAndDetailsGetter, sensorMode string) *ObjectSensor {
	return &ObjectSensor{
		connection: conn,
		sensorMode: sensorMode,
		newStore:   newObjectStore,
	}
}

func (s *ObjectSensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return s.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (s *ObjectSensor) RunTask(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) error {
	if s.sensorMode == "skip" {
		return nil
	}

	bucket, ok := stringParameter(asset, "bucket", "bucket_name")
	if !ok {
		return errors.New("GCS sensor requires a parameter named 'bucket'")
	}

	isPrefixSensor := asset.Type == pipeline.AssetTypeGCSPrefixSensor ||
		asset.Type == pipeline.AssetTypeGCSPrefixSensorLegacy
	targetParameterNames := []string{"object", "object_name", "bucket_key"}
	missingTargetError := "GCS object sensor requires a parameter named 'object'"
	if isPrefixSensor {
		targetParameterNames = []string{"prefix", "object_prefix", "bucket_key"}
		missingTargetError = "GCS prefix sensor requires a parameter named 'prefix'"
	}
	target, ok := stringParameter(asset, targetParameterNames...)
	if !ok {
		return errors.New(missingTargetError)
	}

	connectionName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}
	connectionDetails := s.connection.GetConnectionDetails(connectionName)
	if connectionDetails == nil {
		return config.NewConnectionNotFoundError(ctx, "", connectionName)
	}

	store, err := s.newStore(ctx, connectionDetails)
	if err != nil {
		return fmt.Errorf("failed to initialize GCS sensor connection %q: %w", connectionName, err)
	}
	defer store.Close()

	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		fmt.Fprintln(printer, "Poking GCS:", "gs://"+bucket+"/"+target)
	}

	timeoutDuration := helpers.GetSensorTimeout(asset)
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()

	for {
		var found bool
		if isPrefixSensor {
			found, err = store.ObjectWithPrefixExists(ctx, bucket, target)
		} else if objectpattern.ContainsWildcard(target) {
			found, err = store.ObjectMatchingPatternExists(ctx, bucket, target)
		} else {
			found, err = store.ObjectExists(ctx, bucket, target)
		}
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		if s.sensorMode == "once" || s.sensorMode == "" {
			return errors.New("sensor didn't return the expected result")
		}

		pokeInterval := helpers.GetPokeInterval(ctx, asset)
		pokeTimer := time.NewTimer(time.Duration(pokeInterval) * time.Second)
		select {
		case <-ctx.Done():
			pokeTimer.Stop()
			return ctx.Err()
		case <-timeout.C:
			pokeTimer.Stop()
			return fmt.Errorf("sensor timed out after %s", timeoutDuration)
		case <-pokeTimer.C:
			if printerExists {
				fmt.Fprintln(printer, "Info: GCS object not found, waiting for", pokeInterval, "seconds")
			}
		}
	}
}

func stringParameter(asset *pipeline.Asset, names ...string) (string, bool) {
	for _, name := range names {
		value, ok := asset.Parameters.GetString(name)
		if ok && strings.TrimSpace(value) != "" {
			return value, true
		}
	}
	return "", false
}

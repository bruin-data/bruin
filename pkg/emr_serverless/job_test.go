package emr_serverless //nolint

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestBuildJobRunConf(t *testing.T) {
	t.Parallel()
	type testCase struct {
		Name     string
		Job      Job
		Expected *emrserverless.StartJobRunInput
	}

	testCases := []testCase{
		{
			Name: "smoke test",
			Job: Job{
				params: &JobRunParams{
					Entrypoint:    "main.py",
					ApplicationID: "app-1234567890",
					ExecutionRole: "arn:aws:iam::123456789012:role/EMRServerless_DefaultRole",
				},
				asset: &pipeline.Asset{
					Name: "test-asset",
				},
			},
			Expected: &emrserverless.StartJobRunInput{
				Name:                    aws.String("test-asset"),
				ExecutionTimeoutMinutes: aws.Int64(0),
				ApplicationId:           aws.String("app-1234567890"),
				ExecutionRoleArn:        aws.String("arn:aws:iam::123456789012:role/EMRServerless_DefaultRole"),
				JobDriver: &types.JobDriverMemberSparkSubmit{
					Value: types.SparkSubmit{
						EntryPoint: aws.String("main.py"),
					},
				},
			},
		},
		{
			Name: "variables in job run config",
			Job: Job{
				params: &JobRunParams{
					Entrypoint:    "main.py",
					ApplicationID: "app-1234567890",
					ExecutionRole: "arn:aws:iam::123456789012:role/EMRServerless_DefaultRole",
				},
				asset: &pipeline.Asset{
					Name: "test-asset",
				},
				env: map[string]string{
					"env": "dev",
				},
			},
			Expected: &emrserverless.StartJobRunInput{
				Name:                    aws.String("test-asset"),
				ExecutionTimeoutMinutes: aws.Int64(0),
				ApplicationId:           aws.String("app-1234567890"),
				ExecutionRoleArn:        aws.String("arn:aws:iam::123456789012:role/EMRServerless_DefaultRole"),
				JobDriver: &types.JobDriverMemberSparkSubmit{
					Value: types.SparkSubmit{
						EntryPoint: aws.String("main.py"),
						SparkSubmitParameters: aws.String(
							` --conf spark.executorEnv.env="dev" --conf spark.emr-serverless.driverEnv.env="dev"`,
						),
					},
				},
			},
		},
		{
			Name: "user config + variables in job run config",
			Job: Job{
				params: &JobRunParams{
					Entrypoint:    "main.py",
					ApplicationID: "app-1234567890",
					ExecutionRole: "arn:aws:iam::123456789012:role/EMRServerless_DefaultRole",
					Config:        "--conf spark.executor.cores=1",
				},
				asset: &pipeline.Asset{
					Name: "test-asset",
				},
				env: map[string]string{
					"env": "dev",
				},
			},
			Expected: &emrserverless.StartJobRunInput{
				Name:                    aws.String("test-asset"),
				ExecutionTimeoutMinutes: aws.Int64(0),
				ApplicationId:           aws.String("app-1234567890"),
				ExecutionRoleArn:        aws.String("arn:aws:iam::123456789012:role/EMRServerless_DefaultRole"),
				JobDriver: &types.JobDriverMemberSparkSubmit{
					Value: types.SparkSubmit{
						EntryPoint: aws.String("main.py"),
						SparkSubmitParameters: aws.String(
							`--conf spark.executor.cores=1 --conf spark.executorEnv.env="dev" --conf spark.emr-serverless.driverEnv.env="dev"`,
						),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			actual := tc.Job.buildJobRunConfig()
			if actual == nil {
				t.Fatal("expected non-nil StartJobRunInput")
			}

			assert.Equal(t, tc.Expected, actual)
		})
	}
}

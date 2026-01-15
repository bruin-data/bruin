# Deploying Bruin with AWS Lambda

::: info Managed Option Available
Looking for a fully managed solution? [Bruin Cloud](https://getbruin.com) provides managed orchestration, monitoring, and scheduling without the operational overhead. Try it free!
:::

This guide shows you how to run Bruin pipelines using AWS Lambda functions. This serverless approach is ideal for scheduled or event-driven pipeline executions.

## Prerequisites

Before you begin, ensure you have:
- An AWS account with appropriate permissions
- AWS CLI installed and configured
- Docker installed (for building Lambda container images)
- A Bruin project ready to deploy
- Credentials for your data platforms

## Overview

AWS Lambda allows you to run code without provisioning servers. For Bruin, we'll use:

- **Container images** to package Bruin CLI with your project
- **EventBridge** for scheduling
- **Secrets Manager** for storing credentials
- **CloudWatch Logs** for monitoring

## Approach 1: Lambda with Container Image

### Step 1: Create a Dockerfile

Create a `Dockerfile` in your project root:

```dockerfile
FROM ghcr.io/bruin-data/bruin:latest

# Install AWS Lambda Runtime Interface Client for Python
RUN apk add --no-cache python3 py3-pip && \
    pip3 install --no-cache-dir awslambdaric

# Copy your Bruin project
COPY . /workspace
WORKDIR /workspace

# Copy the Lambda handler
COPY lambda_handler.py /workspace/

# Set the entrypoint
ENTRYPOINT [ "python3", "-m", "awslambdaric" ]
CMD [ "lambda_handler.handler" ]
```

### Step 2: Create Lambda Handler

Create `lambda_handler.py` in your project root:

```python
import subprocess
import json
import os

def handler(event, context):
    """
    AWS Lambda handler to run Bruin pipelines

    Event structure:
    {
        "pipeline": ".",  # Path to pipeline, "." for all
        "environment": "production"
    }
    """

    pipeline = event.get('pipeline', '.')
    environment = event.get('environment', 'production')

    # Get config from environment variable or Secrets Manager
    bruin_config = os.environ.get('BRUIN_CONFIG')
    if bruin_config:
        with open('.bruin.yml', 'w') as f:
            f.write(bruin_config)

    try:
        # Run Bruin command
        cmd = f"bruin run {pipeline} --environment {environment}"
        result = subprocess.run(
            cmd.split(),
            capture_output=True,
            text=True,
            timeout=840  # 14 minutes (Lambda max is 15 min)
        )

        return {
            'statusCode': 200 if result.returncode == 0 else 500,
            'body': json.dumps({
                'message': 'Pipeline execution completed' if result.returncode == 0 else 'Pipeline execution failed',
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode
            })
        }
    except subprocess.TimeoutExpired:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Pipeline execution timed out',
                'error': 'Execution exceeded Lambda timeout'
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Pipeline execution error',
                'error': str(e)
            })
        }
```

### Step 3: Build and Push Docker Image to ECR

```bash
# Set your AWS account ID and region
AWS_ACCOUNT_ID=123456789012
AWS_REGION=us-east-1
REPOSITORY_NAME=bruin-lambda

# Create ECR repository
aws ecr create-repository \
    --repository-name ${REPOSITORY_NAME} \
    --region ${AWS_REGION}

# Authenticate Docker to ECR
aws ecr get-login-password --region ${AWS_REGION} | \
    docker login --username AWS --password-stdin \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Build Docker image
docker build -t ${REPOSITORY_NAME}:latest .

# Tag image
docker tag ${REPOSITORY_NAME}:latest \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPOSITORY_NAME}:latest

# Push to ECR
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPOSITORY_NAME}:latest
```

### Step 4: Store Credentials in AWS Secrets Manager

```bash
# Create a secret with your .bruin.yml content
aws secretsmanager create-secret \
    --name bruin-config \
    --description "Bruin pipeline configuration" \
    --secret-string file://.bruin.yml \
    --region ${AWS_REGION}
```

### Step 5: Create IAM Role for Lambda

Create an IAM role with the following trust policy (`trust-policy.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Create the role and attach policies:

```bash
# Create the role
aws iam create-role \
    --role-name BruinLambdaRole \
    --assume-role-policy-document file://trust-policy.json

# Attach basic Lambda execution policy
aws iam attach-role-policy \
    --role-name BruinLambdaRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# Create and attach policy for Secrets Manager access
cat > secrets-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:bruin-config-*"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name BruinLambdaRole \
    --policy-name SecretsManagerAccess \
    --policy-document file://secrets-policy.json
```

### Step 6: Create Lambda Function

```bash
aws lambda create-function \
    --function-name bruin-pipeline \
    --package-type Image \
    --code ImageUri=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPOSITORY_NAME}:latest \
    --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/BruinLambdaRole \
    --timeout 900 \
    --memory-size 2048 \
    --region ${AWS_REGION}
```

### Step 7: Configure Environment Variables

If storing config as environment variable instead of Secrets Manager:

```bash
# Read .bruin.yml and encode it
BRUIN_CONFIG=$(cat .bruin.yml | base64)

aws lambda update-function-configuration \
    --function-name bruin-pipeline \
    --environment "Variables={BRUIN_CONFIG=${BRUIN_CONFIG}}" \
    --region ${AWS_REGION}
```

Or use Secrets Manager (recommended):

Update `lambda_handler.py` to fetch from Secrets Manager:

```python
import boto3
import json
import subprocess
import os

def get_secret():
    """Retrieve secret from AWS Secrets Manager"""
    secret_name = "bruin-config"
    region_name = os.environ.get('AWS_REGION', 'us-east-1')

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    response = client.get_secret_value(SecretId=secret_name)
    return response['SecretString']

def handler(event, context):
    pipeline = event.get('pipeline', '.')
    environment = event.get('environment', 'production')

    # Get config from Secrets Manager
    bruin_config = get_secret()
    with open('.bruin.yml', 'w') as f:
        f.write(bruin_config)

    try:
        cmd = f"bruin run {pipeline} --environment {environment}"
        result = subprocess.run(
            cmd.split(),
            capture_output=True,
            text=True,
            timeout=840
        )

        return {
            'statusCode': 200 if result.returncode == 0 else 500,
            'body': json.dumps({
                'message': 'Pipeline execution completed' if result.returncode == 0 else 'Pipeline execution failed',
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Pipeline execution error',
                'error': str(e)
            })
        }
```

Rebuild and push the updated image, then update the Lambda function.

## Approach 2: Lambda Layer with Bruin Binary

For simpler projects, you can create a Lambda Layer with Bruin CLI:

### Step 1: Create Lambda Layer

```bash
# Create directory structure
mkdir -p bruin-layer/bin

# Download Bruin binary
curl -L https://github.com/bruin-data/bruin/releases/download/latest/bruin_Linux_x86_64.tar.gz \
    -o bruin.tar.gz

# Extract binary
tar -xzf bruin.tar.gz -C bruin-layer/bin/

# Create layer zip
cd bruin-layer
zip -r ../bruin-layer.zip .
cd ..

# Create Lambda layer
aws lambda publish-layer-version \
    --layer-name bruin-cli \
    --description "Bruin CLI for Lambda" \
    --zip-file fileb://bruin-layer.zip \
    --compatible-runtimes python3.11 python3.12 \
    --region ${AWS_REGION}
```

### Step 2: Create Lambda Function with Layer

Create `lambda_function.py`:

```python
import subprocess
import json
import os

def lambda_handler(event, context):
    pipeline = event.get('pipeline', '.')
    environment = event.get('environment', 'production')

    # Write config
    bruin_config = os.environ.get('BRUIN_CONFIG')
    if bruin_config:
        with open('/tmp/.bruin.yml', 'w') as f:
            f.write(bruin_config)

    # Copy project files to /tmp (Lambda writeable directory)
    subprocess.run(['cp', '-r', '.', '/tmp/workspace'])
    os.chdir('/tmp/workspace')

    if bruin_config:
        subprocess.run(['mv', '/tmp/.bruin.yml', '.bruin.yml'])

    try:
        # Add layer bin to PATH
        os.environ['PATH'] = '/opt/bin:' + os.environ['PATH']

        cmd = f"bruin run {pipeline} --environment {environment}"
        result = subprocess.run(
            cmd.split(),
            capture_output=True,
            text=True,
            timeout=840
        )

        return {
            'statusCode': 200 if result.returncode == 0 else 500,
            'body': json.dumps({
                'message': 'Success' if result.returncode == 0 else 'Failed',
                'output': result.stdout + result.stderr
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
```

Zip and deploy:

```bash
zip -r function.zip lambda_function.py your-bruin-project/

aws lambda create-function \
    --function-name bruin-pipeline-layer \
    --runtime python3.11 \
    --role arn:aws:iam::${AWS_ACCOUNT_ID}:role/BruinLambdaRole \
    --handler lambda_function.lambda_handler \
    --zip-file fileb://function.zip \
    --timeout 900 \
    --memory-size 2048 \
    --layers arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:layer:bruin-cli:1 \
    --region ${AWS_REGION}
```

## Scheduling with EventBridge

### Create EventBridge Rule

Schedule daily at 3 AM UTC:

```bash
aws events put-rule \
    --name bruin-daily-run \
    --description "Run Bruin pipeline daily at 3 AM" \
    --schedule-expression "cron(0 3 * * ? *)" \
    --region ${AWS_REGION}

# Add Lambda as target
aws events put-targets \
    --rule bruin-daily-run \
    --targets "Id"="1","Arn"="arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:bruin-pipeline" \
    --region ${AWS_REGION}

# Grant EventBridge permission to invoke Lambda
aws lambda add-permission \
    --function-name bruin-pipeline \
    --statement-id bruin-daily-run \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn arn:aws:events:${AWS_REGION}:${AWS_ACCOUNT_ID}:rule/bruin-daily-run \
    --region ${AWS_REGION}
```

### Schedule with Specific Pipeline

Create a rule with custom input:

```bash
aws events put-targets \
    --rule bruin-daily-run \
    --targets '[{
        "Id": "1",
        "Arn": "arn:aws:lambda:'${AWS_REGION}':'${AWS_ACCOUNT_ID}':function:bruin-pipeline",
        "Input": "{\"pipeline\": \"pipelines/analytics\", \"environment\": \"production\"}"
    }]' \
    --region ${AWS_REGION}
```

## Multiple Pipelines with Step Functions

For complex workflows, use AWS Step Functions:

### Step 1: Create State Machine Definition

Create `state-machine.json`:

```json
{
  "Comment": "Bruin pipeline orchestration",
  "StartAt": "Validate",
  "States": {
    "Validate": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:123456789012:function:bruin-pipeline",
      "Parameters": {
        "pipeline": ".",
        "environment": "production",
        "command": "validate"
      },
      "Next": "Ingestion"
    },
    "Ingestion": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:123456789012:function:bruin-pipeline",
      "Parameters": {
        "pipeline": "pipelines/ingestion",
        "environment": "production"
      },
      "Next": "Parallel"
    },
    "Parallel": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "Analytics",
          "States": {
            "Analytics": {
              "Type": "Task",
              "Resource": "arn:aws:lambda:us-east-1:123456789012:function:bruin-pipeline",
              "Parameters": {
                "pipeline": "pipelines/analytics",
                "environment": "production"
              },
              "End": true
            }
          }
        },
        {
          "StartAt": "Reporting",
          "States": {
            "Reporting": {
              "Type": "Task",
              "Resource": "arn:aws:lambda:us-east-1:123456789012:function:bruin-pipeline",
              "Parameters": {
                "pipeline": "pipelines/reporting",
                "environment": "production"
              },
              "End": true
            }
          }
        }
      ],
      "End": true
    }
  }
}
```

### Step 2: Create Step Functions State Machine

```bash
# Create IAM role for Step Functions
cat > sfn-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
    --role-name BruinStepFunctionsRole \
    --assume-role-policy-document file://sfn-trust-policy.json

# Create policy to invoke Lambda
cat > sfn-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:bruin-pipeline"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name BruinStepFunctionsRole \
    --policy-name LambdaInvokePolicy \
    --policy-document file://sfn-policy.json

# Create state machine
aws stepfunctions create-state-machine \
    --name bruin-pipeline-orchestrator \
    --definition file://state-machine.json \
    --role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/BruinStepFunctionsRole \
    --region ${AWS_REGION}
```

## Monitoring and Logging

### View Logs in CloudWatch

```bash
# View recent logs
aws logs tail /aws/lambda/bruin-pipeline --follow --region ${AWS_REGION}

# View logs for specific execution
aws logs filter-log-events \
    --log-group-name /aws/lambda/bruin-pipeline \
    --start-time $(date -d '1 hour ago' +%s)000 \
    --region ${AWS_REGION}
```

### Set Up CloudWatch Alarms

```bash
aws cloudwatch put-metric-alarm \
    --alarm-name bruin-pipeline-errors \
    --alarm-description "Alert on Bruin pipeline errors" \
    --metric-name Errors \
    --namespace AWS/Lambda \
    --statistic Sum \
    --period 300 \
    --threshold 1 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 \
    --dimensions Name=FunctionName,Value=bruin-pipeline \
    --region ${AWS_REGION}
```

## Best Practices

### 1. Use Container Images for Full Control

Container images provide better control over dependencies and the runtime environment.

### 2. Set Appropriate Timeouts and Memory

```bash
aws lambda update-function-configuration \
    --function-name bruin-pipeline \
    --timeout 900 \
    --memory-size 2048
```

### 3. Use Secrets Manager for Credentials

Never store credentials in environment variables or code.

### 4. Enable X-Ray Tracing

```bash
aws lambda update-function-configuration \
    --function-name bruin-pipeline \
    --tracing-config Mode=Active
```

### 5. Use VPC for Database Access

If connecting to databases in a VPC:

```bash
aws lambda update-function-configuration \
    --function-name bruin-pipeline \
    --vpc-config SubnetIds=subnet-xxx,subnet-yyy,SecurityGroupIds=sg-xxx
```

### 6. Tag Your Resources

```bash
aws lambda tag-resource \
    --resource arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:bruin-pipeline \
    --tags Project=Bruin,Environment=Production
```

## Troubleshooting

### Lambda Timeout

If pipelines take longer than 15 minutes, consider:
- Breaking pipelines into smaller chunks
- Using AWS Batch or ECS for long-running tasks
- Optimizing your SQL queries

### Memory Issues

Increase Lambda memory:

```bash
aws lambda update-function-configuration \
    --function-name bruin-pipeline \
    --memory-size 4096
```

### Permission Errors

Ensure IAM role has necessary permissions for:
- Secrets Manager access
- CloudWatch Logs
- VPC access (if applicable)
- Data platform access

### Container Image Too Large

Lambda has a 10GB image size limit. To reduce size:
- Use multi-stage builds
- Remove unnecessary files
- Use Alpine base images

## Cost Optimization

### 1. Use Provisioned Concurrency Sparingly

Only for latency-sensitive workloads.

### 2. Optimize Memory Allocation

Test different memory settings to find the cost-effective sweet spot.

### 3. Use Lambda Insights

Monitor costs with AWS Lambda Insights:

```bash
aws lambda update-function-configuration \
    --function-name bruin-pipeline \
    --layers arn:aws:lambda:${AWS_REGION}:580247275435:layer:LambdaInsightsExtension:14
```

## Next Steps

- Explore [Bruin Cloud](/cloud/overview) for managed orchestration
- Learn about [AWS ECS deployment](/deployment/cloud/aws-ecs) for longer-running tasks
- Review [quality checks](/quality/overview) to add validation

## Additional Resources

- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [AWS Step Functions](https://docs.aws.amazon.com/step-functions/)
- [Bruin Docker Images](https://github.com/bruin-data/bruin/pkgs/container/bruin)
- [Bruin CLI Documentation](/)

# Deploying Bruin with AWS ECS

::: info Managed Option Available
Looking for a fully managed solution? [Bruin Cloud](https://getbruin.com) provides managed orchestration, monitoring, and scheduling without the operational overhead. Try it free!
:::

This guide shows you how to run Bruin pipelines using AWS ECS (Elastic Container Service). ECS provides container orchestration for longer-running tasks and complex workflows.

## Prerequisites

Before you begin, ensure you have:
- An AWS account with appropriate permissions
- AWS CLI installed and configured
- Docker installed
- A Bruin project ready to deploy
- Credentials for your data platforms

## Overview

AWS ECS allows you to run Docker containers at scale. For Bruin, we'll use:

- **ECS Fargate** for serverless container execution
- **ECR** for storing Docker images
- **EventBridge** for scheduling
- **Secrets Manager** for storing credentials
- **CloudWatch** for monitoring

## Step 1: Create ECR Repository

```bash
# Set your AWS account ID and region
AWS_ACCOUNT_ID=123456789012
AWS_REGION=us-east-1

# Create ECR repository
aws ecr create-repository \
    --repository-name bruin-pipeline \
    --region ${AWS_REGION}

# Authenticate Docker to ECR
aws ecr get-login-password --region ${AWS_REGION} | \
    docker login --username AWS --password-stdin \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
```

## Step 2: Create Dockerfile

Create a `Dockerfile` in your project root:

```dockerfile
FROM ghcr.io/bruin-data/bruin:latest

# Copy your Bruin project
COPY . /workspace
WORKDIR /workspace

# Create entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
```

Create `entrypoint.sh`:

```bash
#!/bin/sh
set -e

# Get configuration from environment or AWS Secrets Manager
if [ -n "$AWS_SECRET_NAME" ]; then
    echo "Fetching configuration from AWS Secrets Manager..."
    BRUIN_CONFIG=$(aws secretsmanager get-secret-value \
        --secret-id $AWS_SECRET_NAME \
        --query SecretString \
        --output text \
        --region ${AWS_REGION:-us-east-1})
    echo "$BRUIN_CONFIG" > .bruin.yml
fi

# Default values
PIPELINE=${PIPELINE:-.}
ENVIRONMENT=${ENVIRONMENT:-production}
COMMAND=${COMMAND:-run}

# Execute Bruin command
echo "Executing: bruin $COMMAND $PIPELINE --environment $ENVIRONMENT"
exec bruin $COMMAND $PIPELINE --environment $ENVIRONMENT
```

## Step 3: Build and Push Docker Image

```bash
# Build image
docker build -t bruin-pipeline:latest .

# Tag image for ECR
docker tag bruin-pipeline:latest \
    ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/bruin-pipeline:latest

# Push to ECR
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/bruin-pipeline:latest
```

## Step 4: Store Credentials in AWS Secrets Manager

```bash
# Create secret with your .bruin.yml content
aws secretsmanager create-secret \
    --name bruin-config \
    --description "Bruin pipeline configuration" \
    --secret-string file://.bruin.yml \
    --region ${AWS_REGION}
```

## Step 5: Create ECS Task Definition

Create `task-definition.json`:

```json
{
  "family": "bruin-pipeline",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "arn:aws:iam::123456789012:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::123456789012:role/bruinTaskRole",
  "containerDefinitions": [
    {
      "name": "bruin",
      "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/bruin-pipeline:latest",
      "essential": true,
      "environment": [
        {
          "name": "AWS_SECRET_NAME",
          "value": "bruin-config"
        },
        {
          "name": "AWS_REGION",
          "value": "us-east-1"
        },
        {
          "name": "PIPELINE",
          "value": "."
        },
        {
          "name": "ENVIRONMENT",
          "value": "production"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/bruin-pipeline",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "bruin"
        }
      }
    }
  ]
}
```

## Step 6: Create IAM Roles

### Task Execution Role

This role is used by ECS to pull images and write logs:

```bash
# Create trust policy
cat > ecs-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create execution role
aws iam create-role \
    --role-name ecsTaskExecutionRole \
    --assume-role-policy-document file://ecs-trust-policy.json

# Attach AWS managed policy
aws iam attach-role-policy \
    --role-name ecsTaskExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
```

### Task Role

This role is used by your container to access AWS services:

```bash
# Create task role
aws iam create-role \
    --role-name bruinTaskRole \
    --assume-role-policy-document file://ecs-trust-policy.json

# Create policy for Secrets Manager access
cat > bruin-task-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:bruin-config-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name bruinTaskRole \
    --policy-name BruinTaskPolicy \
    --policy-document file://bruin-task-policy.json
```

## Step 7: Create CloudWatch Log Group

```bash
aws logs create-log-group \
    --log-group-name /ecs/bruin-pipeline \
    --region ${AWS_REGION}
```

## Step 8: Register Task Definition

```bash
aws ecs register-task-definition \
    --cli-input-json file://task-definition.json \
    --region ${AWS_REGION}
```

## Step 9: Create ECS Cluster

```bash
aws ecs create-cluster \
    --cluster-name bruin-cluster \
    --region ${AWS_REGION}
```

## Running Tasks

### Run Task Manually

```bash
aws ecs run-task \
    --cluster bruin-cluster \
    --task-definition bruin-pipeline \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-12345678],securityGroups=[sg-12345678],assignPublicIp=ENABLED}" \
    --region ${AWS_REGION}
```

### Run Specific Pipeline

Override environment variables:

```bash
aws ecs run-task \
    --cluster bruin-cluster \
    --task-definition bruin-pipeline \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-12345678],securityGroups=[sg-12345678],assignPublicIp=ENABLED}" \
    --overrides '{
      "containerOverrides": [{
        "name": "bruin",
        "environment": [
          {"name": "PIPELINE", "value": "pipelines/analytics"},
          {"name": "ENVIRONMENT", "value": "production"}
        ]
      }]
    }' \
    --region ${AWS_REGION}
```

## Scheduling with EventBridge

### Create EventBridge Rule

```bash
# Create rule for daily execution at 3 AM UTC
aws events put-rule \
    --name bruin-daily-run \
    --description "Run Bruin pipeline daily at 3 AM" \
    --schedule-expression "cron(0 3 * * ? *)" \
    --region ${AWS_REGION}

# Create IAM role for EventBridge to run ECS tasks
cat > eventbridge-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

aws iam create-role \
    --role-name ecsEventsRole \
    --assume-role-policy-document file://eventbridge-trust-policy.json

cat > eventbridge-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:RunTask"
      ],
      "Resource": "arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:task-definition/bruin-pipeline:*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::${AWS_ACCOUNT_ID}:role/ecsTaskExecutionRole",
        "arn:aws:iam::${AWS_ACCOUNT_ID}:role/bruinTaskRole"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name ecsEventsRole \
    --policy-name ECSEventsPolicy \
    --policy-document file://eventbridge-policy.json

# Add target
aws events put-targets \
    --rule bruin-daily-run \
    --targets "Id"="1",\
"Arn"="arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:cluster/bruin-cluster",\
"RoleArn"="arn:aws:iam::${AWS_ACCOUNT_ID}:role/ecsEventsRole",\
"EcsParameters"="{TaskDefinitionArn=arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:task-definition/bruin-pipeline,TaskCount=1,LaunchType=FARGATE,NetworkConfiguration={awsvpcConfiguration={Subnets=[subnet-12345678],SecurityGroups=[sg-12345678],AssignPublicIp=ENABLED}}}" \
    --region ${AWS_REGION}
```

## Using Step Functions for Complex Workflows

### Create Step Functions State Machine

Create `state-machine.json`:

```json
{
  "Comment": "Bruin pipeline orchestration with ECS",
  "StartAt": "Validate",
  "States": {
    "Validate": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "LaunchType": "FARGATE",
        "Cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/bruin-cluster",
        "TaskDefinition": "arn:aws:ecs:us-east-1:123456789012:task-definition/bruin-pipeline",
        "NetworkConfiguration": {
          "AwsvpcConfiguration": {
            "Subnets": ["subnet-12345678"],
            "SecurityGroups": ["sg-12345678"],
            "AssignPublicIp": "ENABLED"
          }
        },
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "bruin",
              "Environment": [
                {"Name": "COMMAND", "Value": "validate"},
                {"Name": "PIPELINE", "Value": "."}
              ]
            }
          ]
        }
      },
      "Next": "Ingestion"
    },
    "Ingestion": {
      "Type": "Task",
      "Resource": "arn:aws:states:::ecs:runTask.sync",
      "Parameters": {
        "LaunchType": "FARGATE",
        "Cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/bruin-cluster",
        "TaskDefinition": "arn:aws:ecs:us-east-1:123456789012:task-definition/bruin-pipeline",
        "NetworkConfiguration": {
          "AwsvpcConfiguration": {
            "Subnets": ["subnet-12345678"],
            "SecurityGroups": ["sg-12345678"],
            "AssignPublicIp": "ENABLED"
          }
        },
        "Overrides": {
          "ContainerOverrides": [
            {
              "Name": "bruin",
              "Environment": [
                {"Name": "PIPELINE", "Value": "pipelines/ingestion"}
              ]
            }
          ]
        }
      },
      "Next": "ParallelProcessing"
    },
    "ParallelProcessing": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "Analytics",
          "States": {
            "Analytics": {
              "Type": "Task",
              "Resource": "arn:aws:states:::ecs:runTask.sync",
              "Parameters": {
                "LaunchType": "FARGATE",
                "Cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/bruin-cluster",
                "TaskDefinition": "arn:aws:ecs:us-east-1:123456789012:task-definition/bruin-pipeline",
                "NetworkConfiguration": {
                  "AwsvpcConfiguration": {
                    "Subnets": ["subnet-12345678"],
                    "SecurityGroups": ["sg-12345678"],
                    "AssignPublicIp": "ENABLED"
                  }
                },
                "Overrides": {
                  "ContainerOverrides": [
                    {
                      "Name": "bruin",
                      "Environment": [
                        {"Name": "PIPELINE", "Value": "pipelines/analytics"}
                      ]
                    }
                  ]
                }
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
              "Resource": "arn:aws:states:::ecs:runTask.sync",
              "Parameters": {
                "LaunchType": "FARGATE",
                "Cluster": "arn:aws:ecs:us-east-1:123456789012:cluster/bruin-cluster",
                "TaskDefinition": "arn:aws:ecs:us-east-1:123456789012:task-definition/bruin-pipeline",
                "NetworkConfiguration": {
                  "AwsvpcConfiguration": {
                    "Subnets": ["subnet-12345678"],
                    "SecurityGroups": ["sg-12345678"],
                    "AssignPublicIp": "ENABLED"
                  }
                },
                "Overrides": {
                  "ContainerOverrides": [
                    {
                      "Name": "bruin",
                      "Environment": [
                        {"Name": "PIPELINE", "Value": "pipelines/reporting"}
                      ]
                    }
                  ]
                }
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

Deploy the state machine:

```bash
# Create role for Step Functions
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
    --role-name StepFunctionsECSRole \
    --assume-role-policy-document file://sfn-trust-policy.json

cat > sfn-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:RunTask",
        "ecs:StopTask",
        "ecs:DescribeTasks"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:PassRole"
      ],
      "Resource": [
        "arn:aws:iam::${AWS_ACCOUNT_ID}:role/ecsTaskExecutionRole",
        "arn:aws:iam::${AWS_ACCOUNT_ID}:role/bruinTaskRole"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "events:PutTargets",
        "events:PutRule",
        "events:DescribeRule"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name StepFunctionsECSRole \
    --policy-name StepFunctionsECSPolicy \
    --policy-document file://sfn-policy.json

# Create state machine
aws stepfunctions create-state-machine \
    --name bruin-pipeline-orchestrator \
    --definition file://state-machine.json \
    --role-arn arn:aws:iam::${AWS_ACCOUNT_ID}:role/StepFunctionsECSRole \
    --region ${AWS_REGION}
```

## Using ECS Service for Long-Running Tasks

For continuously running pipelines (e.g., streaming):

```bash
aws ecs create-service \
    --cluster bruin-cluster \
    --service-name bruin-streaming \
    --task-definition bruin-pipeline \
    --desired-count 1 \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-12345678],securityGroups=[sg-12345678],assignPublicIp=ENABLED}" \
    --region ${AWS_REGION}
```

## Monitoring and Logging

### View Logs

```bash
# List log streams
aws logs describe-log-streams \
    --log-group-name /ecs/bruin-pipeline \
    --order-by LastEventTime \
    --descending \
    --max-items 10 \
    --region ${AWS_REGION}

# View logs
aws logs tail /ecs/bruin-pipeline --follow --region ${AWS_REGION}
```

### Create CloudWatch Dashboard

```bash
cat > dashboard.json <<EOF
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["AWS/ECS", "CPUUtilization", {"stat": "Average"}],
          [".", "MemoryUtilization", {"stat": "Average"}]
        ],
        "period": 300,
        "stat": "Average",
        "region": "${AWS_REGION}",
        "title": "ECS Resource Utilization"
      }
    }
  ]
}
EOF

aws cloudwatch put-dashboard \
    --dashboard-name BruinPipeline \
    --dashboard-body file://dashboard.json \
    --region ${AWS_REGION}
```

## Auto Scaling

Configure auto scaling for your ECS service:

```bash
# Register scalable target
aws application-autoscaling register-scalable-target \
    --service-namespace ecs \
    --resource-id service/bruin-cluster/bruin-streaming \
    --scalable-dimension ecs:service:DesiredCount \
    --min-capacity 1 \
    --max-capacity 10 \
    --region ${AWS_REGION}

# Create scaling policy
aws application-autoscaling put-scaling-policy \
    --service-namespace ecs \
    --resource-id service/bruin-cluster/bruin-streaming \
    --scalable-dimension ecs:service:DesiredCount \
    --policy-name bruin-cpu-scaling \
    --policy-type TargetTrackingScaling \
    --target-tracking-scaling-policy-configuration '{
      "TargetValue": 75.0,
      "PredefinedMetricSpecification": {
        "PredefinedMetricType": "ECSServiceAverageCPUUtilization"
      },
      "ScaleInCooldown": 300,
      "ScaleOutCooldown": 60
    }' \
    --region ${AWS_REGION}
```

## Best Practices

### 1. Use Fargate Spot for Cost Savings

For non-critical workloads:

```bash
aws ecs run-task \
    --cluster bruin-cluster \
    --task-definition bruin-pipeline \
    --capacity-provider-strategy capacityProvider=FARGATE_SPOT,weight=1 \
    --network-configuration "awsvpcConfiguration={subnets=[subnet-12345678],securityGroups=[sg-12345678],assignPublicIp=ENABLED}"
```

### 2. Set Resource Limits

Optimize CPU and memory in task definition:

```json
{
  "cpu": "2048",
  "memory": "4096"
}
```

### 3. Use Task Metadata Endpoint

Access task metadata from within containers:

```bash
TASK_METADATA=$(curl ${ECS_CONTAINER_METADATA_URI_V4}/task)
```

### 4. Implement Health Checks

Add health checks to your task definition:

```json
{
  "healthCheck": {
    "command": ["CMD-SHELL", "bruin validate . || exit 1"],
    "interval": 30,
    "timeout": 5,
    "retries": 3,
    "startPeriod": 60
  }
}
```

### 5. Tag Resources

```bash
aws ecs tag-resource \
    --resource-arn arn:aws:ecs:${AWS_REGION}:${AWS_ACCOUNT_ID}:task-definition/bruin-pipeline:1 \
    --tags key=Project,value=Bruin key=Environment,value=Production
```

## Troubleshooting

### Task Fails to Start

Check task stopped reason:

```bash
aws ecs describe-tasks \
    --cluster bruin-cluster \
    --tasks TASK_ID \
    --query 'tasks[0].stopReason' \
    --region ${AWS_REGION}
```

### Cannot Pull ECR Image

Ensure task execution role has ECR permissions:

```bash
aws iam attach-role-policy \
    --role-name ecsTaskExecutionRole \
    --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
```

### Network Issues

Verify:
- Subnets have route to internet (NAT Gateway or Internet Gateway)
- Security group allows outbound traffic
- Task has public IP if accessing internet directly

### High Costs

- Use Fargate Spot for non-critical tasks
- Right-size CPU and memory
- Set appropriate task timeout
- Use auto-scaling to scale down when idle

## Next Steps

- Explore [Bruin Cloud](/cloud/overview) for managed orchestration
- Learn about [AWS Lambda](/deployment/cloud/aws-lambda) for shorter tasks
- Review [quality checks](/quality/overview) to add validation

## Additional Resources

- [AWS ECS Documentation](https://docs.aws.amazon.com/ecs/)
- [AWS Fargate Documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html)
- [Bruin Docker Images](https://github.com/bruin-data/bruin/pkgs/container/bruin)
- [Bruin CLI Documentation](/)

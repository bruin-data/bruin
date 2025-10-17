# Deploying Bruin with Apache Airflow

::: info Managed Option Available
Looking for a fully managed solution? [Bruin Cloud](https://getbruin.com) provides managed orchestration, monitoring, and scheduling without the operational overhead. Try it free!
:::

This guide shows you how to orchestrate Bruin pipelines using Apache Airflow. You can use Airflow to schedule and monitor your Bruin pipelines with either the BashOperator or KubernetesOperator.

## Prerequisites

Before you begin, ensure you have:
- Apache Airflow installed and running
- Access to your Airflow environment
- A Bruin project ready to deploy
- Credentials for your data platforms

## Overview

Apache Airflow is a platform to programmatically author, schedule, and monitor workflows. You can integrate Bruin with Airflow using:

- **BashOperator**: Run Bruin CLI directly on the Airflow worker nodes
- **KubernetesOperator**: Run Bruin in isolated Kubernetes pods using official Bruin Docker images

## Using BashOperator

The BashOperator runs Bruin CLI directly on Airflow worker nodes. This approach is simple and works well for smaller deployments.

### Step 1: Install Bruin on Airflow Workers

Install Bruin CLI on all Airflow worker nodes:

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

Verify the installation:

```bash
bruin --version
```

### Step 2: Set Up Your Bruin Project

Clone your Bruin project to a location accessible by Airflow workers:

```bash
# On each Airflow worker node
cd /opt/airflow
git clone https://github.com/your-username/your-bruin-project.git
```

### Step 3: Configure Credentials

Create a `.bruin.yml` file in your project directory with your credentials:

```bash
nano /opt/airflow/your-bruin-project/.bruin.yml
```

Example configuration:

```yaml
environments:
  production:
    connections:
      postgres:
        - name: "my_postgres"
          username: "postgres_user"
          password: "your_password"
          host: "your-db-host.com"
          port: 5432
          database: "mydb"

      google_cloud_platform:
        - name: "my_gcp"
          service_account_json: |
            {
              "type": "service_account",
              "project_id": "my-project-id",
              "private_key_id": "...",
              "private_key": "...",
              "client_email": "...",
              "client_id": "..."
            }
          project_id: "my-project-id"
```

Secure the file:

```bash
chmod 600 /opt/airflow/your-bruin-project/.bruin.yml
```

### Step 4: Create an Airflow DAG

Create a DAG file in your Airflow DAGs folder (e.g., `~/airflow/dags/bruin_pipeline.py`):

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bruin_pipeline',
    default_args=default_args,
    description='Run Bruin data pipeline',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    catchup=False,
    tags=['bruin', 'data-pipeline'],
)

# Run the entire pipeline
run_pipeline = BashOperator(
    task_id='run_bruin_pipeline',
    bash_command='cd /opt/airflow/your-bruin-project && bruin run . --environment production',
    dag=dag,
)

run_pipeline
```

### Step 5: Running Specific Pipelines

You can create tasks for specific pipelines and set dependencies:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bruin_multi_pipeline',
    default_args=default_args,
    description='Run multiple Bruin pipelines with dependencies',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['bruin', 'data-pipeline'],
)

# Validate before running
validate = BashOperator(
    task_id='validate_pipeline',
    bash_command='cd /opt/airflow/your-bruin-project && bruin validate .',
    dag=dag,
)

# Run ingestion pipeline
ingestion = BashOperator(
    task_id='run_ingestion',
    bash_command='cd /opt/airflow/your-bruin-project && bruin run pipelines/ingestion --environment production',
    dag=dag,
)

# Run analytics pipeline
analytics = BashOperator(
    task_id='run_analytics',
    bash_command='cd /opt/airflow/your-bruin-project && bruin run pipelines/analytics --environment production',
    dag=dag,
)

# Run reporting pipeline
reporting = BashOperator(
    task_id='run_reporting',
    bash_command='cd /opt/airflow/your-bruin-project && bruin run pipelines/reporting --environment production',
    dag=dag,
)

# Set dependencies
validate >> ingestion >> analytics >> reporting
```

### Step 6: Using Airflow Variables for Configuration

Store sensitive configuration in Airflow Variables or Connections:

```python
from airflow.models import Variable

# Create .bruin.yml from Airflow Variable
create_config = BashOperator(
    task_id='create_bruin_config',
    bash_command=f'echo \'{Variable.get("bruin_config")}\' > /opt/airflow/your-bruin-project/.bruin.yml',
    dag=dag,
)

run_pipeline = BashOperator(
    task_id='run_bruin_pipeline',
    bash_command='cd /opt/airflow/your-bruin-project && bruin run . --environment production',
    dag=dag,
)

create_config >> run_pipeline
```

To set the variable in Airflow UI:
1. Go to **Admin** → **Variables**
2. Create a new variable named `bruin_config`
3. Paste your entire `.bruin.yml` content as the value

## Using KubernetesOperator

The KubernetesOperator runs Bruin in isolated Kubernetes pods using the official Bruin Docker images. This approach provides better isolation, scalability, and resource management.

### Prerequisites

- Airflow running on Kubernetes or with access to a Kubernetes cluster
- KubernetesPodOperator installed: `pip install apache-airflow-providers-cncf-kubernetes`

### Step 1: Create a Kubernetes Secret for Credentials

Store your `.bruin.yml` as a Kubernetes Secret:

```bash
# Create the secret from your .bruin.yml file
kubectl create secret generic bruin-config \
  --from-file=.bruin.yml=/path/to/your/.bruin.yml \
  -n airflow
```

Or create it from a YAML manifest:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: bruin-config
  namespace: airflow
type: Opaque
stringData:
  .bruin.yml: |
    environments:
      production:
        connections:
          postgres:
            - name: "my_postgres"
              username: "postgres_user"
              password: "your_password"
              host: "your-db-host.com"
              port: 5432
              database: "mydb"
```

Apply it:

```bash
kubectl apply -f bruin-secret.yaml
```

### Step 2: Create an Airflow DAG with KubernetesPodOperator

Create a DAG file using the official Bruin Docker image:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bruin_kubernetes_pipeline',
    default_args=default_args,
    description='Run Bruin pipeline in Kubernetes',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['bruin', 'kubernetes'],
)

# Mount the config secret
volume_mount = k8s.V1VolumeMount(
    name='bruin-config',
    mount_path='/config',
    read_only=True
)

volume = k8s.V1Volume(
    name='bruin-config',
    secret=k8s.V1SecretVolumeSource(secret_name='bruin-config')
)

# Run Bruin pipeline
run_pipeline = KubernetesPodOperator(
    task_id='run_bruin_pipeline',
    name='bruin-pipeline',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[
        'cp /config/.bruin.yml /bruin-project/.bruin.yml && '
        'bruin run . --environment production'
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

run_pipeline
```

### Step 3: Clone Your Bruin Project in the Pod

For projects stored in Git, clone the repository at runtime:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bruin_kubernetes_git',
    default_args=default_args,
    description='Run Bruin pipeline from Git repository',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['bruin', 'kubernetes', 'git'],
)

# Mount the config secret
volume_mount = k8s.V1VolumeMount(
    name='bruin-config',
    mount_path='/config',
    read_only=True
)

volume = k8s.V1Volume(
    name='bruin-config',
    secret=k8s.V1SecretVolumeSource(secret_name='bruin-config')
)

# Run Bruin pipeline from Git
run_pipeline = KubernetesPodOperator(
    task_id='run_bruin_from_git',
    name='bruin-git-pipeline',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[
        'apk add --no-cache git && '
        'git clone https://github.com/your-username/your-bruin-project.git /workspace && '
        'cd /workspace && '
        'cp /config/.bruin.yml .bruin.yml && '
        'bruin run . --environment production'
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

run_pipeline
```

### Step 4: Using Git SSH Keys for Private Repositories

For private repositories, add SSH keys as a Kubernetes Secret:

```bash
# Create SSH key secret
kubectl create secret generic git-ssh-key \
  --from-file=id_rsa=/path/to/your/private/key \
  -n airflow
```

Update your DAG:

```python
# Mount SSH key
ssh_volume_mount = k8s.V1VolumeMount(
    name='git-ssh-key',
    mount_path='/root/.ssh',
    read_only=True
)

ssh_volume = k8s.V1Volume(
    name='git-ssh-key',
    secret=k8s.V1SecretVolumeSource(
        secret_name='git-ssh-key',
        default_mode=0o600
    )
)

run_pipeline = KubernetesPodOperator(
    task_id='run_bruin_from_private_git',
    name='bruin-private-git',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[
        'apk add --no-cache git openssh && '
        'ssh-keyscan github.com >> /root/.ssh/known_hosts && '
        'git clone git@github.com:your-username/your-private-repo.git /workspace && '
        'cd /workspace && '
        'cp /config/.bruin.yml .bruin.yml && '
        'bruin run . --environment production'
    ],
    volumes=[volume, ssh_volume],
    volume_mounts=[volume_mount, ssh_volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)
```

### Step 5: Running Multiple Pipelines with Dependencies

Create a DAG with multiple tasks and dependencies:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bruin_k8s_multi_pipeline',
    default_args=default_args,
    description='Run multiple Bruin pipelines in Kubernetes',
    schedule_interval='0 3 * * *',
    catchup=False,
    tags=['bruin', 'kubernetes'],
)

volume_mount = k8s.V1VolumeMount(
    name='bruin-config',
    mount_path='/config',
    read_only=True
)

volume = k8s.V1Volume(
    name='bruin-config',
    secret=k8s.V1SecretVolumeSource(secret_name='bruin-config')
)

# Base command to set up the environment
base_cmd = (
    'apk add --no-cache git && '
    'git clone https://github.com/your-username/your-bruin-project.git /workspace && '
    'cd /workspace && '
    'cp /config/.bruin.yml .bruin.yml && '
)

# Validate pipeline
validate = KubernetesPodOperator(
    task_id='validate',
    name='bruin-validate',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[base_cmd + 'bruin validate .'],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Ingestion pipeline
ingestion = KubernetesPodOperator(
    task_id='ingestion',
    name='bruin-ingestion',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[base_cmd + 'bruin run pipelines/ingestion --environment production'],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Analytics pipeline
analytics = KubernetesPodOperator(
    task_id='analytics',
    name='bruin-analytics',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[base_cmd + 'bruin run pipelines/analytics --environment production'],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Reporting pipeline
reporting = KubernetesPodOperator(
    task_id='reporting',
    name='bruin-reporting',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[base_cmd + 'bruin run pipelines/reporting --environment production'],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

# Set dependencies
validate >> ingestion >> [analytics, reporting]
```

### Step 6: Resource Allocation

Specify resource requests and limits for your pods:

```python
run_pipeline = KubernetesPodOperator(
    task_id='run_bruin_pipeline',
    name='bruin-pipeline',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[
        'cp /config/.bruin.yml /workspace/.bruin.yml && '
        'bruin run . --environment production'
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    resources=k8s.V1ResourceRequirements(
        requests={
            'memory': '512Mi',
            'cpu': '500m',
        },
        limits={
            'memory': '2Gi',
            'cpu': '2000m',
        }
    ),
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)
```

## Using Specific Bruin Docker Image Versions

The official Bruin Docker images are available at: https://github.com/bruin-data/bruin/pkgs/container/bruin

You can use specific versions for production stability:

```python
# Use a specific version (recommended for production)
image='ghcr.io/bruin-data/bruin:v0.11.324'

# Use latest (not recommended for production)
image='ghcr.io/bruin-data/bruin:latest'
```

## Advanced Configuration

### Using Init Containers

Use init containers to prepare the environment:

```python
init_container = k8s.V1Container(
    name='git-clone',
    image='alpine/git:latest',
    command=['sh', '-c'],
    args=[
        'git clone https://github.com/your-username/your-bruin-project.git /workspace'
    ],
    volume_mounts=[
        k8s.V1VolumeMount(
            name='workspace',
            mount_path='/workspace'
        )
    ]
)

workspace_volume = k8s.V1Volume(
    name='workspace',
    empty_dir=k8s.V1EmptyDirVolumeSource()
)

workspace_mount = k8s.V1VolumeMount(
    name='workspace',
    mount_path='/workspace'
)

run_pipeline = KubernetesPodOperator(
    task_id='run_bruin_pipeline',
    name='bruin-pipeline',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    cmds=['sh', '-c'],
    arguments=[
        'cd /workspace && '
        'cp /config/.bruin.yml .bruin.yml && '
        'bruin run . --environment production'
    ],
    init_containers=[init_container],
    volumes=[volume, workspace_volume],
    volume_mounts=[volume_mount, workspace_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)
```

### Environment Variables

Pass environment variables to your Bruin containers:

```python
run_pipeline = KubernetesPodOperator(
    task_id='run_bruin_pipeline',
    name='bruin-pipeline',
    namespace='airflow',
    image='ghcr.io/bruin-data/bruin:latest',
    env_vars={
        'BRUIN_ENV': 'production',
        'LOG_LEVEL': 'info',
    },
    cmds=['sh', '-c'],
    arguments=[
        'cp /config/.bruin.yml /workspace/.bruin.yml && '
        'bruin run . --environment $BRUIN_ENV'
    ],
    volumes=[volume],
    volume_mounts=[volume_mount],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)
```

## Monitoring and Logging

### Viewing Logs in Airflow UI

Both BashOperator and KubernetesPodOperator logs are available in the Airflow UI:

1. Go to your DAG in the Airflow UI
2. Click on a task instance
3. Click **Log** to view the execution logs

### Sending Notifications on Failure

Configure email or Slack notifications:

```python
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'airflow',
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# Or use a callback for Slack
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def slack_failure_callback(context):
    slack_msg = f"""
    :x: Task Failed
    *Task*: {context.get('task_instance').task_id}
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log Url*: {context.get('task_instance').log_url}
    """

    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg,
    )

    return failed_alert.execute(context=context)

dag = DAG(
    'bruin_pipeline',
    default_args=default_args,
    on_failure_callback=slack_failure_callback,
    schedule_interval='0 3 * * *',
)
```

## Best Practices

### 1. Use Specific Image Versions

Always pin to specific Bruin versions in production:

```python
image='ghcr.io/bruin-data/bruin:v0.11.324'
```

### 2. Store Credentials Securely

- Use Airflow Variables or Connections for BashOperator
- Use Kubernetes Secrets for KubernetesPodOperator
- Never hardcode credentials in DAG files

### 3. Set Appropriate Retries

Configure retries for transient failures:

```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}
```

### 4. Use Task Dependencies

Define clear dependencies between tasks:

```python
validate >> ingestion >> [analytics, reporting]
```

### 5. Monitor Resource Usage

For KubernetesPodOperator, set appropriate resource limits to prevent resource exhaustion.

### 6. Clean Up Pods

Always set `is_delete_operator_pod=True` to clean up completed pods.

## Troubleshooting

### BashOperator Issues

**Bruin command not found:**
- Ensure Bruin is installed on all worker nodes
- Verify PATH includes `~/.local/bin`

**Permission denied:**
- Check file permissions on `.bruin.yml`
- Ensure the Airflow user has access to the project directory

### KubernetesPodOperator Issues

**ImagePullBackOff:**
- Verify the image name and tag are correct
- Check if you need image pull secrets for private registries

**Pod stuck in pending:**
- Check resource requests/limits
- Verify the namespace exists
- Check node availability

**Secret not found:**
- Verify the secret exists in the correct namespace
- Check secret name in the volume configuration

**Git clone fails:**
- Verify repository URL is correct
- For private repos, ensure SSH keys are properly mounted
- Check network policies allow outbound connections

## Next Steps

- Explore [Bruin Cloud](/cloud/overview) for managed orchestration
- Learn about [quality checks](/quality/overview) to add validation tasks
- Review [CI/CD integration](/cicd/github-action) for testing DAGs

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Bruin Docker Images](https://github.com/bruin-data/bruin/pkgs/container/bruin)
- [Bruin CLI Documentation](/)
- [Credentials Management](/getting-started/credentials)

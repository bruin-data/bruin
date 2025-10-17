# Deploying Bruin on Ubuntu VMs

This guide walks you through deploying Bruin on Ubuntu-based virtual machines (AWS EC2, Google Cloud Compute Engine, DigitalOcean Droplets, or any Ubuntu server) and scheduling pipeline runs using cron jobs.

## Prerequisites

Before you begin, ensure you have:
- An Ubuntu server (18.04 or later recommended)
- SSH access to the server with sudo privileges
- Git installed on the server
- A Bruin project ready to deploy

## Step 1: Connect to Your Server

Connect to your Ubuntu VM via SSH:

```bash
ssh username@your-server-ip
```

Replace `username` with your actual username and `your-server-ip` with your server's IP address or hostname.

## Step 2: Update System Packages

Always start by updating your system packages:

```bash
sudo apt update && sudo apt upgrade -y
```

## Step 3: Install Git (if not already installed)

Git is required to clone your Bruin projects:

```bash
sudo apt install git -y
```

Verify the installation:

```bash
git --version
```

## Step 4: Install Bruin CLI

Install Bruin using the official installation script:

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

Alternatively, you can use `wget`:

```bash
wget -qO- https://getbruin.com/install/cli | sh
```

The installer will automatically add Bruin to your PATH. You may need to restart your shell or run:

```bash
source ~/.bashrc  # or ~/.zshrc if using zsh
```

Verify the installation:

```bash
bruin --version
```

## Step 5: Clone Your Bruin Project

Clone your Bruin project repository to your server:

```bash
cd ~
git clone https://github.com/your-username/your-bruin-project.git
cd your-bruin-project
```

Replace the URL with your actual repository URL.

## Step 6: Configure Credentials

Bruin needs access to your data platforms. Set up your credentials in the `.bruin.yml` file in your project root.

Create or edit the `.bruin.yml` file:

```bash
nano .bruin.yml
```

Example configuration:

```yaml
environments:
  production:
    connections:
      google_cloud_platform:
        - name: "my_gcp"
          service_account_file: "/home/username/.config/gcloud/service-account.json"
          project_id: "my-project-id"

      postgres:
        - name: "my_postgres"
          username: "postgres_user"
          password: "your_password"
          host: "localhost"
          port: 5432
          database: "mydb"
```

### Storing Service Account Files

If you're using service account files (e.g., for Google Cloud):

```bash
mkdir -p ~/.config/gcloud
nano ~/.config/gcloud/service-account.json
```

Paste your service account JSON content, save, and secure the file:

```bash
chmod 600 ~/.config/gcloud/service-account.json
```

## Step 7: Test Your Pipeline

Before setting up automation, test that your pipeline runs successfully:

```bash
cd ~/your-bruin-project
bruin run .
```

If you want to run a specific pipeline:

```bash
bruin run pipelines/my_pipeline
```

Check for any errors and resolve them before proceeding.

## Step 8: Set Up Cron Jobs

Cron is a time-based job scheduler in Unix-like operating systems. You'll use it to run your Bruin pipelines automatically.

### Understanding Cron Syntax

Cron uses the following format:

```
* * * * * command-to-execute
│ │ │ │ │
│ │ │ │ └─── Day of week (0-7, Sunday = 0 or 7)
│ │ │ └───── Month (1-12)
│ │ └─────── Day of month (1-31)
│ └───────── Hour (0-23)
└─────────── Minute (0-59)
```

Examples:
- `0 * * * *` - Every hour at minute 0
- `0 9 * * *` - Every day at 9:00 AM
- `*/15 * * * *` - Every 15 minutes
- `0 2 * * 1` - Every Monday at 2:00 AM
- `0 0 1 * *` - First day of every month at midnight

### Create a Cron Job

Open your crontab file:

```bash
crontab -e
```

If this is your first time, you'll be asked to choose an editor. Select `nano` (option 1) for simplicity.

Add a cron job to run your pipeline. Here's an example that runs daily at 3:00 AM:

```bash
0 3 * * * /home/username/.local/bin/bruin run /home/username/your-bruin-project >> /home/username/logs/bruin.log 2>&1
```

**Important notes:**
- Use absolute paths for both the Bruin executable and your project directory
- Replace `username` with your actual username
- The `>> /home/username/logs/bruin.log 2>&1` redirects output to a log file

### Multiple Pipelines with Different Schedules

You can schedule different pipelines at different times:

```bash
# Run data ingestion pipeline every hour
0 * * * * /home/username/.local/bin/bruin run /home/username/your-bruin-project/pipelines/ingestion >> /home/username/logs/ingestion.log 2>&1

# Run analytics pipeline daily at 6 AM
0 6 * * * /home/username/.local/bin/bruin run /home/username/your-bruin-project/pipelines/analytics >> /home/username/logs/analytics.log 2>&1

# Run weekly report every Monday at 8 AM
0 8 * * 1 /home/username/.local/bin/bruin run /home/username/your-bruin-project/pipelines/weekly_report >> /home/username/logs/weekly.log 2>&1
```

## Step 9: Set Up Logging

Create a directory for logs:

```bash
mkdir -p ~/logs
```

Your cron jobs will now write outputs to log files in this directory.

### View Logs

Check recent logs:

```bash
tail -f ~/logs/bruin.log
```

View last 100 lines:

```bash
tail -n 100 ~/logs/bruin.log
```

Search for errors:

```bash
grep -i error ~/logs/bruin.log
```

### Log Rotation

To prevent log files from growing too large, set up log rotation:

```bash
sudo nano /etc/logrotate.d/bruin
```

Add the following configuration:

```
/home/username/logs/*.log {
    daily
    missingok
    rotate 14
    compress
    notifempty
    create 0644 username username
}
```

This configuration:
- Rotates logs daily
- Keeps 14 days of logs
- Compresses old logs
- Creates new log files with proper permissions

## Step 10: Set Up Environment-Specific Configurations

Use Bruin's environment feature to manage different configurations:

```bash
bruin run . --environment production
```

Update your cron job to use the production environment:

```bash
0 3 * * * /home/username/.local/bin/bruin run /home/username/your-bruin-project --environment production >> /home/username/logs/bruin.log 2>&1
```

## Step 11: Monitoring and Alerting

### Email Notifications on Failure

Cron can send emails when jobs fail. First, install a mail utility:

```bash
sudo apt install mailutils -y
```

Configure postfix when prompted (select "Internet Site").

Create a wrapper script to handle errors:

```bash
nano ~/scripts/run-bruin.sh
```

Add the following:

```bash
#!/bin/bash

LOG_FILE="/home/username/logs/bruin.log"
PROJECT_PATH="/home/username/your-bruin-project"
BRUIN_BIN="/home/username/.local/bin/bruin"

echo "=== Starting Bruin run at $(date) ===" >> "$LOG_FILE"

if ! $BRUIN_BIN run "$PROJECT_PATH" --environment production >> "$LOG_FILE" 2>&1; then
    echo "Bruin pipeline failed at $(date)" | mail -s "Bruin Pipeline Failed" your-email@example.com
    exit 1
fi

echo "=== Completed successfully at $(date) ===" >> "$LOG_FILE"
```

Make it executable:

```bash
chmod +x ~/scripts/run-bruin.sh
```

Update your crontab:

```bash
0 3 * * * /home/username/scripts/run-bruin.sh
```

## Step 12: Automatic Updates

Keep your Bruin project up to date by pulling changes before each run:

Update your wrapper script:

```bash
#!/bin/bash

LOG_FILE="/home/username/logs/bruin.log"
PROJECT_PATH="/home/username/your-bruin-project"
BRUIN_BIN="/home/username/.local/bin/bruin"

echo "=== Starting Bruin run at $(date) ===" >> "$LOG_FILE"

# Pull latest changes
cd "$PROJECT_PATH"
git pull origin main >> "$LOG_FILE" 2>&1

# Run the pipeline
if ! $BRUIN_BIN run "$PROJECT_PATH" --environment production >> "$LOG_FILE" 2>&1; then
    echo "Bruin pipeline failed at $(date)" | mail -s "Bruin Pipeline Failed" your-email@example.com
    exit 1
fi

echo "=== Completed successfully at $(date) ===" >> "$LOG_FILE"
```

## Security Best Practices

### 1. Secure Your Credentials

Never commit credentials to Git:

```bash
echo ".bruin.yml" >> .gitignore
echo "*.json" >> .gitignore
```

### 2. Use SSH Keys for Git

Set up SSH keys for passwordless Git operations:

```bash
ssh-keygen -t ed25519 -C "your-email@example.com"
cat ~/.ssh/id_ed25519.pub
```

Add the public key to your Git provider (GitHub, GitLab, etc.).

### 3. Restrict File Permissions

```bash
chmod 600 ~/.bruin.yml
chmod 600 ~/.config/gcloud/*.json
```

### 4. Use a Dedicated User

Create a dedicated user for running Bruin:

```bash
sudo useradd -m -s /bin/bash bruin
sudo su - bruin
```

Then follow all the installation steps as the `bruin` user.

## Troubleshooting

### Cron Job Not Running

1. Check if cron service is running:
```bash
sudo systemctl status cron
```

2. Check cron logs:
```bash
grep CRON /var/log/syslog
```

3. Verify your crontab:
```bash
crontab -l
```

### Bruin Command Not Found in Cron

Cron has a limited environment. Always use absolute paths:

```bash
# Find the full path to bruin
which bruin

# Use the full path in crontab
0 3 * * * /home/username/.local/bin/bruin run /home/username/your-bruin-project
```

### Permission Denied Errors

Ensure your user has permission to access all files:

```bash
chmod +x ~/.local/bin/bruin
chmod -R 755 ~/your-bruin-project
```

### Connection Issues

Test your connections:

```bash
bruin connections --environment production
```

### Pipeline Fails in Cron but Works Manually

This often happens due to environment differences. Export all necessary environment variables in your wrapper script:

```bash
#!/bin/bash
export PATH="/home/username/.local/bin:$PATH"
export HOME="/home/username"
# Add other environment variables here

# Run your pipeline
/home/username/.local/bin/bruin run /home/username/your-bruin-project
```

## Example: Complete Production Setup

Here's a complete example for a production deployment:

### Directory Structure
```
/home/bruin/
├── projects/
│   └── analytics-pipeline/
├── scripts/
│   ├── run-ingestion.sh
│   └── run-analytics.sh
├── logs/
│   ├── ingestion.log
│   └── analytics.log
└── .config/
    └── gcloud/
        └── service-account.json
```

### Crontab
```bash
# Pull and run ingestion pipeline every 6 hours
0 */6 * * * /home/bruin/scripts/run-ingestion.sh

# Run analytics pipeline daily at 2 AM
0 2 * * * /home/bruin/scripts/run-analytics.sh
```

### run-analytics.sh
```bash
#!/bin/bash

set -e

export PATH="/home/bruin/.local/bin:$PATH"
export HOME="/home/bruin"

LOG_FILE="/home/bruin/logs/analytics.log"
PROJECT_PATH="/home/bruin/projects/analytics-pipeline"

echo "=== Starting analytics run at $(date) ===" >> "$LOG_FILE"

cd "$PROJECT_PATH"
git pull origin main >> "$LOG_FILE" 2>&1

if ! bruin run . --environment production >> "$LOG_FILE" 2>&1; then
    echo "Analytics pipeline failed at $(date)" | mail -s "Alert: Analytics Pipeline Failed" admin@company.com
    exit 1
fi

echo "=== Completed successfully at $(date) ===" >> "$LOG_FILE"
```

## Next Steps

- Explore [Bruin Cloud](/cloud/overview) for managed orchestration and monitoring
- Set up [CI/CD integration](/cicd/github-action) for automated testing
- Learn about [quality checks](/quality/overview) to ensure data quality
- Review [best practices](/getting-started/design-principles) for pipeline design

## Additional Resources

- [Bruin CLI Documentation](/)
- [Bruin Commands](/commands/run)
- [Credentials Management](/getting-started/credentials)

# Deployment Guide

## Overview

This guide covers deploying the Crypto Wealth Tracker in various environments, from local development to production cloud deployments.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Local Development](#local-development)
- [Production Deployment](#production-deployment)
- [Cloud Deployments](#cloud-deployments)
- [Monitoring and Maintenance](#monitoring-and-maintenance)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

**Minimum Requirements:**
- Python 3.13+
- 2 GB RAM
- 10 GB disk space
- Internet connectivity

**Recommended for Production:**
- Python 3.13+
- 4+ GB RAM
- 50+ GB disk space
- Redis instance
- Load balancer (for multiple instances)

### Required Accounts and API Keys

1. **Alchemy Account**
   - Sign up at [alchemy.com](https://alchemy.com)
   - Create a new app for Ethereum Mainnet
   - Copy the API key

2. **CoinGecko Account (Optional)**
   - Sign up at [coingecko.com](https://coingecko.com)
   - Get API key for higher rate limits
   - Free tier works for basic usage

3. **Google Cloud Account**
   - Create project in [Google Cloud Console](https://console.cloud.google.com)
   - Enable Google Sheets API
   - Create service account and download credentials

4. **Redis Instance (Recommended)**
   - Local Redis installation, or
   - Cloud Redis service (AWS ElastiCache, Google Cloud Memorystore, etc.)

## Environment Setup

### 1. Python Environment

Using UV (Recommended):
```bash
# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone repository
git clone https://github.com/CiDsTaR/crypto-wealth-tracker.git
cd crypto-wealth-tracker

# Install dependencies
uv sync --all-extras
```

Using pip:
```bash
# Clone repository
git clone https://github.com/CiDsTaR/crypto-wealth-tracker.git
cd crypto-wealth-tracker

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .
```

### 2. Environment Variables

Create `.env` file in the project root:

```bash
# Copy template
cp .env.template .env

# Edit with your values
nano .env
```

**Required Environment Variables:**

```bash
# Application Settings
ENVIRONMENT=development
DEBUG=true
DRY_RUN=false
LOG_LEVEL=INFO

# Ethereum Configuration
ALCHEMY_API_KEY=your_alchemy_api_key_here
ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/your_key
ALCHEMY_RATE_LIMIT=100

# CoinGecko Configuration (Optional)
COINGECKO_API_KEY=your_coingecko_api_key
COINGECKO_BASE_URL=https://api.coingecko.com/api/v3
COINGECKO_RATE_LIMIT=30

# Google Sheets Configuration
GOOGLE_SHEETS_CREDENTIALS_FILE=config/google_sheets_credentials.json
GOOGLE_SHEETS_SCOPE=https://www.googleapis.com/auth/spreadsheets

# Cache Configuration
CACHE_BACKEND=hybrid
REDIS_URL=redis://localhost:6379/0
REDIS_PASSWORD=
FILE_CACHE_DIR=cache
CACHE_TTL_PRICES=3600
CACHE_TTL_BALANCES=1800
CACHE_MAX_SIZE_MB=500

# Processing Configuration
BATCH_SIZE=50
MAX_CONCURRENT_REQUESTS=10
REQUEST_DELAY=0.1
INACTIVE_WALLET_THRESHOLD_DAYS=365
RETRY_ATTEMPTS=3
RETRY_DELAY=1.0

# Logging Configuration
LOG_FILE=logs/wallet_tracker.log
LOG_MAX_SIZE_MB=100
LOG_BACKUP_COUNT=5
```

### 3. Google Sheets Setup

1. **Create Service Account:**
   ```bash
   # Go to Google Cloud Console
   # Navigate to IAM & Admin > Service Accounts
   # Create new service account
   # Generate and download JSON key
   ```

2. **Place Credentials:**
   ```bash
   mkdir -p config
   mv ~/Downloads/service-account-key.json config/google_sheets_credentials.json
   ```

3. **Share Spreadsheets:**
   - Open your Google Sheets
   - Share with service account email
   - Grant "Editor" permissions

### 4. Redis Setup

**Local Redis (Development):**
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install redis-server
sudo systemctl start redis-server

# macOS
brew install redis
brew services start redis

# Docker
docker run -d --name redis -p 6379:6379 redis:alpine
```

**Redis Configuration:**
```bash
# Test connection
redis-cli ping
# Should return: PONG
```

## Local Development

### 1. Validate Configuration

```bash
# Validate configuration
uv run python -m wallet_tracker.cli validate --check-config --check-credentials

# Test Google Sheets access
uv run python -m wallet_tracker.cli validate --check-sheets YOUR_SPREADSHEET_ID
```

### 2. Run Health Checks

```bash
# Check all services
uv run python -m wallet_tracker.cli health

# Check specific components
uv run python -m wallet_tracker.cli health --format json
```

### 3. Test Basic Functionality

```bash
# Interactive mode
uv run python -m wallet_tracker.cli interactive

# Analyze from command line
uv run python -m wallet_tracker.cli analyze \
  --spreadsheet-id YOUR_SPREADSHEET_ID \
  --input-range "A:B" \
  --output-range "A1" \
  --dry-run
```

### 4. Development Server

```bash
# Run with development settings
ENVIRONMENT=development uv run python -m wallet_tracker.main --interactive
```

## Production Deployment

### 1. Production Environment Variables

```bash
# Production .env
ENVIRONMENT=production
DEBUG=false
DRY_RUN=false
LOG_LEVEL=INFO

# Use production Redis
REDIS_URL=redis://your-production-redis:6379/0
REDIS_PASSWORD=your_redis_password

# Higher limits for production
ALCHEMY_RATE_LIMIT=500
BATCH_SIZE=100
MAX_CONCURRENT_REQUESTS=20

# Production logging
LOG_FILE=/var/log/wallet-tracker/app.log
LOG_LEVEL=WARNING
```

### 2. Systemd Service (Linux)

Create `/etc/systemd/system/wallet-tracker.service`:

```ini
[Unit]
Description=Crypto Wealth Tracker
After=network.target redis.service
Requires=redis.service

[Service]
Type=simple
User=wallet-tracker
Group=wallet-tracker
WorkingDirectory=/opt/wallet-tracker
Environment=PATH=/opt/wallet-tracker/venv/bin
EnvironmentFile=/opt/wallet-tracker/.env
ExecStart=/opt/wallet-tracker/venv/bin/python -m wallet_tracker.main
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Enable and start service:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable wallet-tracker
sudo systemctl start wallet-tracker
sudo systemctl status wallet-tracker
```

### 3. Process Management with Supervisor

Install Supervisor:
```bash
sudo apt install supervisor
```

Create `/etc/supervisor/conf.d/wallet-tracker.conf`:
```ini
[program:wallet-tracker]
command=/opt/wallet-tracker/venv/bin/python -m wallet_tracker.main
directory=/opt/wallet-tracker
user=wallet-tracker
autostart=true
autorestart=true
stdout_logfile=/var/log/supervisor/wallet-tracker.log
stderr_logfile=/var/log/supervisor/wallet-tracker-error.log
environment=PATH="/opt/wallet-tracker/venv/bin"
```

**Start with Supervisor:**
```bash
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start wallet-tracker
```

### 4. Nginx Reverse Proxy (If exposing HTTP API)

Create `/etc/nginx/sites-available/wallet-tracker`:
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

**Enable site:**
```bash
sudo ln -s /etc/nginx/sites-available/wallet-tracker /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## Cloud Deployments

### Google Cloud Run

1. **Prepare Docker Image:**

Create `Dockerfile`:
```dockerfile
FROM python:3.13-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY pyproject.toml ./
COPY src/ ./src/

# Install UV and dependencies
RUN pip install uv
RUN uv sync --no-dev

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash app
RUN chown -R app:app /app
USER app

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PORT=8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import asyncio; from wallet_tracker.main import get_app; app = get_app(); asyncio.run(app.get_health_status())"

# Run application
CMD ["python", "-m", "wallet_tracker.main"]
```

2. **Build and Deploy:**

```bash
# Build image
gcloud builds submit --tag gcr.io/PROJECT_ID/wallet-tracker

# Deploy to Cloud Run
gcloud run deploy wallet-tracker \
  --image gcr.io/PROJECT_ID/wallet-tracker \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --max-instances 10 \
  --set-env-vars ENVIRONMENT=production \
  --set-env-vars ALCHEMY_API_KEY=$ALCHEMY_API_KEY \
  --set-env-vars REDIS_URL=$REDIS_URL
```

3. **Environment Setup:**

```bash
# Set up secrets
echo -n "$ALCHEMY_API_KEY" | gcloud secrets create alchemy-api-key --data-file=-
echo -n "$COINGECKO_API_KEY" | gcloud secrets create coingecko-api-key --data-file=-

# Create service account for secrets access
gcloud iam service-accounts create wallet-tracker-runner

# Grant secret access
gcloud secrets add-iam-policy-binding alchemy-api-key \
  --member="serviceAccount:wallet-tracker-runner@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### AWS ECS Deployment

1. **Create Task Definition:**

Create `task-definition.json`:
```json
{
  "family": "wallet-tracker",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "executionRoleArn": "arn:aws:iam::ACCOUNT:role/ecsTaskExecutionRole",
  "taskRoleArn": "arn:aws:iam::ACCOUNT:role/ecsTaskRole",
  "containerDefinitions": [
    {
      "name": "wallet-tracker",
      "image": "ACCOUNT.dkr.ecr.REGION.amazonaws.com/wallet-tracker:latest",
      "essential": true,
      "portMappings": [
        {
          "containerPort": 8080,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {"name": "ENVIRONMENT", "value": "production"},
        {"name": "REDIS_URL", "value": "redis://your-elasticache-endpoint:6379"}
      ],
      "secrets": [
        {
          "name": "ALCHEMY_API_KEY",
          "valueFrom": "arn:aws:secretsmanager:REGION:ACCOUNT:secret:alchemy-api-key"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/wallet-tracker",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
```

2. **Deploy with ECS:**

```bash
# Register task definition
aws ecs register-task-definition --cli-input-json file://task-definition.json

# Create service
aws ecs create-service \
  --cluster wallet-tracker-cluster \
  --service-name wallet-tracker-service \
  --task-definition wallet-tracker:1 \
  --desired-count 2 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-12345],securityGroups=[sg-12345],assignPublicIp=ENABLED}"
```

### Docker Compose (Development/Testing)

Create `docker-compose.yml`:
```yaml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      - ENVIRONMENT=development
      - REDIS_URL=redis://redis:6379/0
      - ALCHEMY_API_KEY=${ALCHEMY_API_KEY}
      - COINGECKO_API_KEY=${COINGECKO_API_KEY}
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
      - ./cache:/app/cache
    depends_on:
      - redis
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    restart: unless-stopped

  redis-commander:
    image: rediscommander/redis-commander:latest
    environment:
      - REDIS_HOSTS=local:redis:6379
    ports:
      - "8081:8081"
    depends_on:
      - redis

volumes:
  redis_data:
```

**Run with Docker Compose:**
```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f app

# Stop services
docker-compose down
```

## Monitoring and Maintenance

### 1. Health Monitoring

**Automated Health Checks:**
```bash
#!/bin/bash
# health-check.sh

HEALTH_URL="http://localhost:8080/health"
LOG_FILE="/var/log/wallet-tracker/health.log"

response=$(curl -s -o /dev/null -w "%{http_code}" $HEALTH_URL)

if [ $response -eq 200 ]; then
    echo "$(date): Health check passed" >> $LOG_FILE
else
    echo "$(date): Health check failed with code $response" >> $LOG_FILE
    # Send alert (email, Slack, etc.)
    /opt/scripts/send-alert.sh "Wallet Tracker health check failed"
fi
```

**Add to crontab:**
```bash
# Check every 5 minutes
*/5 * * * * /opt/scripts/health-check.sh
```

### 2. Log Management

**Logrotate Configuration (`/etc/logrotate.d/wallet-tracker`):**
```
/var/log/wallet-tracker/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    copytruncate
    notifempty
    postrotate
        systemctl reload wallet-tracker
    endscript
}
```

### 3. Backup Strategy

**Database/Cache Backup:**
```bash
#!/bin/bash
# backup-redis.sh

BACKUP_DIR="/opt/backups/wallet-tracker"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

# Redis backup
redis-cli --rdb $BACKUP_DIR/redis_$DATE.rdb

# Compress and clean old backups
gzip $BACKUP_DIR/redis_$DATE.rdb
find $BACKUP_DIR -name "redis_*.rdb.gz" -mtime +7 -delete
```

### 4. Performance Monitoring

**System Metrics Script:**
```bash
#!/bin/bash
# monitor-performance.sh

LOG_FILE="/var/log/wallet-tracker/performance.log"

# CPU and Memory usage
CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | awk -F'%' '{print $1}')
MEM=$(free | grep Mem | awk '{printf "%.2f", $3/$2 * 100.0}')

# Redis memory usage
REDIS_MEM=$(redis-cli info memory | grep used_memory_human | cut -d: -f2 | tr -d '\r')

# Log metrics
echo "$(date),$CPU,$MEM,$REDIS_MEM" >> $LOG_FILE
```

### 5. Application Metrics

**Custom Metrics Collection:**
```python
# metrics-collector.py
import asyncio
import json
from datetime import datetime
from wallet_tracker.main import get_app

async def collect_metrics():
    """Collect and log application metrics."""
    app = get_app()
    await app.initialize()

    try:
        metrics = await app.get_metrics()

        # Add timestamp
        metrics['timestamp'] = datetime.now(timezone.utc).isoformat()

        # Log to file
        with open('/var/log/wallet-tracker/metrics.json', 'a') as f:
            f.write(json.dumps(metrics) + '\n')

        print(f"Metrics collected at {metrics['timestamp']}")

    finally:
        await app.cleanup()

if __name__ == "__main__":
    asyncio.run(collect_metrics())
```

## Troubleshooting

### Common Issues

#### 1. Redis Connection Issues

**Symptoms:**
- Cache errors in logs
- Slow response times
- "Connection refused" errors

**Solutions:**
```bash
# Check Redis status
systemctl status redis
redis-cli ping

# Check connection settings
echo $REDIS_URL
redis-cli -u $REDIS_URL ping

# Test authentication
redis-cli -u $REDIS_URL auth $REDIS_PASSWORD

# Check Redis logs
tail -f /var/log/redis/redis-server.log
```

#### 2. API Rate Limiting

**Symptoms:**
- "Rate limit exceeded" errors
- HTTP 429 responses
- Slow processing

**Solutions:**
```bash
# Check current rate limits in config
grep RATE_LIMIT .env

# Reduce batch size and increase delays
export BATCH_SIZE=25
export MAX_CONCURRENT_REQUESTS=5
export REQUEST_DELAY=0.5

# Monitor API usage
uv run python -m wallet_tracker.cli metrics --component coingecko_client
```

#### 3. Google Sheets Permissions

**Symptoms:**
- "Permission denied" errors
- "Spreadsheet not found" errors
- Authentication failures

**Solutions:**
```bash
# Verify credentials file exists
ls -la config/google_sheets_credentials.json

# Check service account email
cat config/google_sheets_credentials.json | jq -r .client_email

# Test sheets access
uv run python -m wallet_tracker.cli validate --check-sheets YOUR_SPREADSHEET_ID

# Verify spreadsheet permissions
# 1. Open Google Sheets in browser
# 2. Share with service account email
# 3. Grant "Editor" permissions
```

#### 4. Memory Issues

**Symptoms:**
- Out of memory errors
- Process killed by OOM killer
- Slow performance

**Solutions:**
```bash
# Check memory usage
free -h
top -p $(pgrep -f wallet_tracker)

# Reduce batch size
export BATCH_SIZE=25
export MAX_CONCURRENT_REQUESTS=5

# Clear cache if needed
redis-cli FLUSHDB

# Monitor memory usage
watch -n 5 'free -h && echo "---" && ps aux | grep wallet_tracker'
```

#### 5. Network Connectivity

**Symptoms:**
- "Connection timeout" errors
- DNS resolution failures
- SSL certificate errors

**Solutions:**
```bash
# Test Alchemy connectivity
curl -H "Content-Type: application/json" -d '{"id":1,"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}' https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY

# Test CoinGecko connectivity
curl https://api.coingecko.com/api/v3/ping

# Check DNS resolution
nslookup api.coingecko.com
nslookup eth-mainnet.g.alchemy.com

# Test SSL certificates
openssl s_client -connect api.coingecko.com:443 </dev/null
```

### Debug Mode

**Enable Debug Logging:**
```bash
export LOG_LEVEL=DEBUG
export DEBUG=true

# Run with verbose output
uv run python -m wallet_tracker.cli --log-level DEBUG analyze --spreadsheet-id YOUR_ID --dry-run
```

**Debug Configuration:**
```python
# debug_config.py
import logging
from wallet_tracker.config import get_config

# Enable debug mode
config = get_config()
print(f"Environment: {config.environment}")
print(f"Debug mode: {config.debug}")

# Test configuration validation
from wallet_tracker.config import get_settings
settings = get_settings()
validation = settings.validate_config()
print(f"Config valid: {validation['valid']}")
if validation['issues']:
    print(f"Issues: {validation['issues']}")
```

### Performance Tuning

**Configuration Tuning:**
```bash
# For high-performance processing
export BATCH_SIZE=100
export MAX_CONCURRENT_REQUESTS=20
export REQUEST_DELAY=0.05
export CACHE_TTL_PRICES=7200
export CACHE_TTL_BALANCES=3600

# For stability (slower but more reliable)
export BATCH_SIZE=25
export MAX_CONCURRENT_REQUESTS=5
export REQUEST_DELAY=0.2
export RETRY_ATTEMPTS=5
export RETRY_DELAY=2.0
```

**Resource Monitoring:**
```bash
# Monitor system resources
htop

# Monitor network connections
netstat -an | grep :6379  # Redis
netstat -an | grep :443   # HTTPS connections

# Monitor file descriptors
lsof -p $(pgrep -f wallet_tracker)

# Monitor disk usage
df -h
du -sh cache/ logs/
```

### Log Analysis

**Common Log Patterns:**
```bash
# Find errors in logs
grep -i error /var/log/wallet-tracker/app.log

# Find rate limit issues
grep -i "rate limit" /var/log/wallet-tracker/app.log

# Find failed wallets
grep -i "failed to process" /var/log/wallet-tracker/app.log

# Monitor processing progress
tail -f /var/log/wallet-tracker/app.log | grep -i "progress"

# Find cache statistics
grep -i "cache hit" /var/log/wallet-tracker/app.log
```

**Log Analysis Script:**
```bash
#!/bin/bash
# analyze-logs.sh

LOG_FILE="/var/log/wallet-tracker/app.log"

echo "=== Wallet Tracker Log Analysis ==="
echo "Log file: $LOG_FILE"
echo "Last 24 hours:"

# Count errors
ERRORS=$(grep -c "ERROR" $LOG_FILE)
echo "Errors: $ERRORS"

# Count successful processing
SUCCESS=$(grep -c "successfully processed" $LOG_FILE)
echo "Successful processes: $SUCCESS"

# Count rate limit hits
RATE_LIMITS=$(grep -c -i "rate limit" $LOG_FILE)
echo "Rate limit hits: $RATE_LIMITS"

# Show last 10 errors
if [ $ERRORS -gt 0 ]; then
    echo ""
    echo "=== Recent Errors ==="
    grep "ERROR" $LOG_FILE | tail -10
fi
```

### Recovery Procedures

**Service Recovery:**
```bash
#!/bin/bash
# recover-service.sh

echo "Starting wallet tracker recovery..."

# Stop service
systemctl stop wallet-tracker

# Clear cache if corrupted
redis-cli FLUSHDB

# Check disk space
if [ $(df / | awk 'NR==2 {print $5}' | sed 's/%//') -gt 90 ]; then
    echo "Cleaning up disk space..."
    find /var/log/wallet-tracker -name "*.log" -mtime +7 -delete
    find cache/ -name "*" -mtime +1 -delete
fi

# Restart Redis if needed
systemctl restart redis

# Update configuration if needed
# cp /opt/configs/production.env .env

# Restart service
systemctl start wallet-tracker

# Wait and check status
sleep 10
systemctl status wallet-tracker

echo "Recovery completed. Check logs for any issues."
```

**Database Recovery:**
```bash
#!/bin/bash
# recover-redis.sh

BACKUP_DIR="/opt/backups/wallet-tracker"

echo "Redis recovery options:"
echo "1. Restart Redis service"
echo "2. Restore from backup"
echo "3. Clear all data and restart fresh"

read -p "Choose option (1-3): " choice

case $choice in
    1)
        systemctl restart redis
        ;;
    2)
        systemctl stop redis
        latest_backup=$(ls -t $BACKUP_DIR/redis_*.rdb.gz | head -1)
        gunzip -c $latest_backup > /var/lib/redis/dump.rdb
        chown redis:redis /var/lib/redis/dump.rdb
        systemctl start redis
        ;;
    3)
        systemctl stop redis
        rm -f /var/lib/redis/dump.rdb
        systemctl start redis
        ;;
esac

redis-cli ping
```

## Security Considerations

### 1. API Key Management

- Store API keys in environment variables or secret management systems
- Rotate API keys regularly
- Use least-privilege access for service accounts
- Monitor API key usage for anomalies

### 2. Network Security

- Use HTTPS for all external API calls
- Implement IP whitelisting where possible
- Use VPC/private networks in cloud deployments
- Enable firewall rules to restrict access

### 3. Data Protection

- Encrypt sensitive data at rest
- Use TLS for data in transit
- Implement proper access controls
- Regular security audits and updates

### 4. Monitoring and Alerting

- Monitor for suspicious activity
- Set up alerts for failed authentication
- Track unusual API usage patterns
- Regular log reviews

## Support and Maintenance

### Regular Tasks

**Daily:**
- Check application health
- Review error logs
- Monitor API usage

**Weekly:**
- Analyze performance metrics
- Review and rotate logs
- Update dependencies (if needed)

**Monthly:**
- Security updates
- Configuration review
- Backup testing
- Performance optimization

### Getting Help

1. **Documentation**: Check the full documentation for common issues
2. **Logs**: Always check application and system logs first
3. **GitHub Issues**: Report bugs and request features
4. **Community**: Join discussions and get help from other users

### Maintenance Scripts

Place maintenance scripts in `/opt/scripts/` and make them executable:

```bash
sudo mkdir -p /opt/scripts
sudo cp scripts/*.sh /opt/scripts/
sudo chmod +x /opt/scripts/*.sh

# Add to daily cron
echo "0 2 * * * /opt/scripts/daily-maintenance.sh" | sudo crontab -
```

This deployment guide should get you started with deploying the Crypto Wealth Tracker in any environment. Adjust configurations based on your specific requirements and infrastructure.

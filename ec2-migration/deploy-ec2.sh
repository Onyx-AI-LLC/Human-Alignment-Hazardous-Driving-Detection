#!/bin/bash

# EC2 Server Deployment Script
# Deploys HAHD application from Render to EC2 with S3 backups

set -e

# Configuration
STACK_NAME="hahd-ec2-server"
REGION="us-east-1"
TEMPLATE_FILE="cloudformation/ec2-server-stack.yaml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== HAHD EC2 Server Deployment ===${NC}"

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI not found. Please install it first.${NC}"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo -e "${RED}Error: AWS CLI not configured. Run 'aws configure' first.${NC}"
    exit 1
fi

# Get AWS account info
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo -e "${GREEN}Using AWS Account: ${ACCOUNT_ID}${NC}"

# Check for required parameters
if [ -z "$1" ]; then
    echo -e "${RED}Usage: $0 <key-pair-name> [domain-name] [jwt-secret]${NC}"
    echo -e "${YELLOW}Example: $0 my-key-pair hahd.myapp.com my-secret-key${NC}"
    echo ""
    echo -e "${BLUE}Available key pairs:${NC}"
    aws ec2 describe-key-pairs --query 'KeyPairs[*].KeyName' --output table
    exit 1
fi

KEY_PAIR_NAME="$1"
DOMAIN_NAME="${2:-}"
JWT_SECRET="${3:-$(openssl rand -base64 32)}"

echo -e "${GREEN}Configuration:${NC}"
echo "  Key Pair: $KEY_PAIR_NAME"
echo "  Domain: ${DOMAIN_NAME:-None (will use IP)}"
echo "  Region: $REGION"

# Verify key pair exists
if ! aws ec2 describe-key-pairs --key-names "$KEY_PAIR_NAME" > /dev/null 2>&1; then
    echo -e "${RED}Error: Key pair '$KEY_PAIR_NAME' not found.${NC}"
    echo -e "${YELLOW}Available key pairs:${NC}"
    aws ec2 describe-key-pairs --query 'KeyPairs[*].KeyName' --output table
    exit 1
fi

read -p "$(echo -e ${YELLOW}Do you want to proceed with the deployment? [y/N]: ${NC})" -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi

# Check if stack already exists
STACK_EXISTS="false"
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" > /dev/null 2>&1; then
    STACK_EXISTS="true"
    echo -e "${YELLOW}Stack '$STACK_NAME' already exists. This will update it.${NC}"
    read -p "$(echo -e ${YELLOW}Continue with stack update? [y/N]: ${NC})" -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Deployment cancelled."
        exit 0
    fi
fi

# Deploy CloudFormation stack
echo -e "${YELLOW}Deploying CloudFormation stack...${NC}"
aws cloudformation deploy \
    --template-file "$TEMPLATE_FILE" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION" \
    --parameter-overrides \
        KeyPairName="$KEY_PAIR_NAME" \
        DomainName="$DOMAIN_NAME" \
        JWTSecret="$JWT_SECRET"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ CloudFormation stack deployed successfully!${NC}"
else
    echo -e "${RED}❌ CloudFormation deployment failed${NC}"
    exit 1
fi

# Get stack outputs
echo -e "${YELLOW}Retrieving stack outputs...${NC}"
OUTPUTS=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query 'Stacks[0].Outputs')

PUBLIC_IP=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="PublicIP") | .OutputValue')
PUBLIC_DNS=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="PublicDNS") | .OutputValue')
BACKUP_BUCKET=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="BackupBucketName") | .OutputValue')
SERVER_URL=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="ServerURL") | .OutputValue')
SSH_COMMAND=$(echo "$OUTPUTS" | jq -r '.[] | select(.OutputKey=="SSHCommand") | .OutputValue')

echo -e "${GREEN}✅ Deployment completed successfully!${NC}"

echo -e "${BLUE}=== Server Information ===${NC}"
echo "  Public IP: $PUBLIC_IP"
echo "  Public DNS: $PUBLIC_DNS"
echo "  Server URL: $SERVER_URL"
echo "  Backup Bucket: $BACKUP_BUCKET"
echo "  SSH Command: $SSH_COMMAND"

# Wait for instance to be ready
echo -e "${YELLOW}Waiting for instance to be ready...${NC}"
sleep 60

# Test server connectivity
echo -e "${YELLOW}Testing server connectivity...${NC}"
MAX_RETRIES=10
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -f "http://$PUBLIC_IP" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Server is responding!${NC}"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo -e "${YELLOW}Attempt $RETRY_COUNT/$MAX_RETRIES: Server not ready yet, waiting...${NC}"
        sleep 30
    fi
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo -e "${RED}⚠️  Server may not be ready yet. Check the instance manually.${NC}"
else
    # Test API endpoints
    echo -e "${YELLOW}Testing API endpoints...${NC}"
    
    # Test health endpoint
    if curl -f "http://$PUBLIC_IP/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Health endpoint working${NC}"
    else
        echo -e "${YELLOW}⚠️  Health endpoint not responding${NC}"
    fi
    
    # Test video endpoint
    if curl -f "http://$PUBLIC_IP/api/videos/test-s3" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ S3 video endpoint working${NC}"
    else
        echo -e "${YELLOW}⚠️  S3 video endpoint not responding${NC}"
    fi
fi

# Create frontend configuration
echo -e "${YELLOW}Creating frontend configuration...${NC}"
mkdir -p frontend-config

cat > frontend-config/api-config.js << EOL
// Frontend API Configuration for EC2 Deployment
// Replace the hardcoded URLs in your frontend with these

const API_CONFIG = {
  // Development
  development: {
    baseURL: 'http://localhost:3001'
  },
  
  // Production (your EC2 instance)
  production: {
    baseURL: '${SERVER_URL}'
  },
  
  // Get current environment
  getCurrentConfig() {
    const env = process.env.NODE_ENV || 'development';
    return this[env] || this.development;
  }
};

// Export for use in your React app
export default API_CONFIG;
EOL

cat > frontend-config/update-instructions.md << EOL
# Frontend Configuration Update Instructions

Your EC2 server is now running at: **${SERVER_URL}**

## Required Frontend Changes

### 1. Update API URLs in React Components

Replace all hardcoded URLs in these files:
- \`src/hooks/useSignIn.ts\`
- \`src/hooks/useRegister.ts\` 
- \`src/hooks/useValidateReferral.ts\`
- \`src/hooks/usePostResults.ts\`
- \`src/components/Questions.tsx\`
- \`src/components/VideoPlayer.tsx\`
- \`src/components/ModelVideoPlayer.tsx\`

**Old URL:** \`https://human-alignment-hazardous-driving.onrender.com\`
**New URL:** \`${SERVER_URL}\`

### 2. Create Environment Configuration

Create \`frontend/.env.production\`:
\`\`\`
VITE_API_BASE_URL=${SERVER_URL}
\`\`\`

### 3. Update API Calls Pattern

Instead of:
\`\`\`typescript
const response = await axios.post('https://human-alignment-hazardous-driving.onrender.com/auth/signIn', data);
\`\`\`

Use:
\`\`\`typescript
const API_BASE = import.meta.env.VITE_API_BASE_URL || '${SERVER_URL}';
const response = await axios.post(\`\${API_BASE}/auth/signIn\`, data);
\`\`\`

### 4. Test the Changes

1. Update your frontend code with the new URLs
2. Build your frontend: \`npm run build\`
3. Deploy to HostGator
4. Test all functionality:
   - User registration/login
   - Video playback
   - Survey submission
   - Raffle leaderboard

### 5. CORS Configuration

The server is configured to allow requests from any origin during testing.
For production, you may want to restrict CORS to your HostGator domain.

## Rollback Plan

If anything doesn't work, you can easily rollback:
1. Change the URLs back to: \`https://human-alignment-hazardous-driving.onrender.com\`
2. Redeploy your frontend
3. Your Render server is still running unchanged

## Monitoring

- Server logs: \`ssh -i ~/.ssh/${KEY_PAIR_NAME}.pem ec2-user@${PUBLIC_IP} 'tail -f /opt/hahd/logs/combined.log'\`
- CloudWatch logs: AWS Console > CloudWatch > Log Groups > /aws/hahd/application
- Server status: curl ${SERVER_URL}/health
EOL

echo -e "${GREEN}Frontend configuration created in: frontend-config/${NC}"

# Setup MongoDB backup
echo -e "${YELLOW}Setting up MongoDB backup...${NC}"

# Test MongoDB backup
ssh -i ~/.ssh/${KEY_PAIR_NAME}.pem -o StrictHostKeyChecking=no ec2-user@${PUBLIC_IP} << EOL
cd /opt/hahd
node scripts/mongodb-backup.js health
EOL

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ MongoDB connection verified${NC}"
    
    # Setup cron job for daily backups
    ssh -i ~/.ssh/${KEY_PAIR_NAME}.pem -o StrictHostKeyChecking=no ec2-user@${PUBLIC_IP} << 'EOL'
# Add daily backup cron job (runs at 2 AM UTC)
(crontab -l 2>/dev/null || echo "") | grep -v "mongodb-backup.js" | (cat && echo "0 2 * * * cd /opt/hahd && /usr/bin/node scripts/mongodb-backup.js backup >> /opt/hahd/logs/backup-cron.log 2>&1") | crontab -
echo "✅ Daily backup cron job configured"
EOL
else
    echo -e "${RED}⚠️  MongoDB connection test failed. Check the logs.${NC}"
fi

# Final instructions
echo -e "${BLUE}=== Next Steps ===${NC}"
echo "1. Update your frontend API URLs using the instructions in:"
echo "   📄 frontend-config/update-instructions.md"
echo ""
echo "2. Test your application:"
echo "   🌐 Server health: curl ${SERVER_URL}/health"
echo "   📺 Video test: curl ${SERVER_URL}/api/videos/test-s3"
echo ""
echo "3. Monitor your server:"
echo "   📊 CloudWatch logs: AWS Console > Log Groups"
echo "   📁 Backup status: aws s3 ls s3://${BACKUP_BUCKET}/mongodb-backups/"
echo ""
echo "4. SSH to your server:"
echo "   🔑 ${SSH_COMMAND}"
echo ""
echo -e "${GREEN}🎉 Your EC2 server is ready!${NC}"
echo -e "${YELLOW}💰 Estimated monthly cost: ~\$10-15 (t3.micro instance + storage + data transfer)${NC}"

# Save deployment info
cat > deployment-info.json << EOL
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
  "stack_name": "${STACK_NAME}",
  "public_ip": "${PUBLIC_IP}",
  "public_dns": "${PUBLIC_DNS}",
  "server_url": "${SERVER_URL}",
  "backup_bucket": "${BACKUP_BUCKET}",
  "ssh_command": "${SSH_COMMAND}",
  "region": "${REGION}",
  "key_pair": "${KEY_PAIR_NAME}",
  "domain": "${DOMAIN_NAME}"
}
EOL

echo -e "${BLUE}Deployment information saved to: deployment-info.json${NC}"
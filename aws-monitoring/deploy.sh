#!/bin/bash

# MongoDB Survey Results Monitor Deployment Script
# This script deploys the monitoring infrastructure to AWS

set -e

# Configuration
STACK_NAME="mongodb-survey-monitor"
REGION="us-east-1"
TEMPLATE_FILE="cloudformation/mongodb-monitor-stack.yaml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== MongoDB Survey Monitor Deployment ===${NC}"

# Check if AWS CLI is configured
echo -e "${YELLOW}Checking AWS CLI configuration...${NC}"
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    echo -e "${RED}Error: AWS CLI not configured or no valid credentials${NC}"
    echo "Please run 'aws configure' first"
    exit 1
fi

# Get AWS account info
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo -e "${GREEN}Using AWS Account: ${ACCOUNT_ID}${NC}"

# Create PyMongo layer
echo -e "${YELLOW}Creating PyMongo Lambda layer...${NC}"
mkdir -p layers/pymongo/python
cd layers/pymongo/python

# Install pymongo in the layer directory
pip install pymongo -t .

# Create layer zip
cd ../
zip -r ../../../pymongo-layer.zip python/

cd ../../../

# Create S3 bucket for artifacts (if it doesn't exist)
BUCKET_NAME="mongodb-monitor-artifacts-${ACCOUNT_ID}-${REGION}"
echo -e "${YELLOW}Creating S3 bucket for Lambda artifacts...${NC}"

if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    echo -e "${GREEN}Bucket $BUCKET_NAME already exists${NC}"
else
    aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION"
    echo -e "${GREEN}Created bucket: $BUCKET_NAME${NC}"
fi

# Upload PyMongo layer to S3
echo -e "${YELLOW}Uploading PyMongo layer to S3...${NC}"
aws s3 cp pymongo-layer.zip "s3://$BUCKET_NAME/pymongo-layer.zip"

# Deploy CloudFormation stack
echo -e "${YELLOW}Deploying CloudFormation stack...${NC}"
aws cloudformation deploy \
    --template-file "$TEMPLATE_FILE" \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION" \
    --parameter-overrides \
        MongoDBURI="mongodb+srv://jonahmulcrone:1RFqimbiYxCpSlkU@hahdcluster.igin9.mongodb.net/survey?retryWrites=true&w=majority&appName=HAHDCluster"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Stack deployed successfully!${NC}"
    
    # Get stack outputs
    echo -e "${YELLOW}Getting stack outputs...${NC}"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
        --output table
    
    echo -e "${BLUE}=== Deployment Complete ===${NC}"
    echo -e "${GREEN}Your MongoDB monitor is now active and will:${NC}"
    echo "• Check MongoDB every 5 minutes for new documents"
    echo "• Trigger processing when document count increases by 50"
    echo "• Track progress in DynamoDB table"
    echo "• Publish EventBridge events for downstream processing"
    echo ""
    echo -e "${YELLOW}Next Steps:${NC}"
    echo "1. Monitor CloudWatch logs for the Lambda function"
    echo "2. Set up additional Lambda functions to handle the processing events"
    echo "3. Configure SNS notifications if needed"
    echo ""
    echo -e "${BLUE}Useful Commands:${NC}"
    echo "• View logs: aws logs describe-log-groups --log-group-name-prefix '/aws/lambda/mongodb-survey-monitor'"
    echo "• Check DynamoDB: aws dynamodb scan --table-name survey-results-tracking"
    echo "• Disable monitoring: aws events disable-rule --name mongodb-monitor-schedule"
    
else
    echo -e "${RED}❌ Stack deployment failed${NC}"
    exit 1
fi

# Cleanup
rm -rf layers/ pymongo-layer.zip

echo -e "${GREEN}Deployment completed successfully!${NC}"
# MongoDB Survey Results Monitor

This AWS-based monitoring solution automatically tracks the number of documents in your MongoDB `survey.results` collection and triggers processing when the count increases by 50 documents.

## Architecture

```
MongoDB Atlas → Lambda (every 5 min) → DynamoDB (tracking) → EventBridge → Processing Triggers
```

### Components

1. **Lambda Function** (`mongodb-survey-monitor`)
   - Runs every 5 minutes via EventBridge schedule
   - Connects to MongoDB to count documents
   - Tracks progress in DynamoDB
   - Publishes events when thresholds are reached

2. **DynamoDB Table** (`survey-results-tracking`)
   - Stores current document count and last processed count
   - Tracks timestamps for monitoring

3. **EventBridge Rules**
   - Schedule rule: Triggers Lambda every 5 minutes
   - Processing rule: Listens for threshold events (ready for your processing functions)

4. **S3 Bucket**
   - Stores Lambda deployment artifacts

## Deployment

### Prerequisites
- AWS CLI configured with appropriate permissions
- Python 3.x installed
- pip package manager

### Deploy the Infrastructure

1. Navigate to the monitoring directory:
   ```bash
   cd aws-monitoring
   ```

2. Run the deployment script:
   ```bash
   ./deploy.sh
   ```

This will:
- Create a PyMongo Lambda layer
- Deploy all AWS resources via CloudFormation
- Set up the monitoring schedule

### Test the Deployment

Run the test script to verify everything is working:

```bash
python3 test-monitor.py
```

## How It Works

### Document Counting Logic

1. **Initialization**: On first run, the system records the current document count as baseline
2. **Regular Checks**: Every 5 minutes, Lambda queries MongoDB for current count
3. **Threshold Detection**: When count increases by 50+ since last processing:
   - Calculates how many 50-document batches are ready
   - Publishes EventBridge event with processing details
   - Updates DynamoDB with new processed count

### Example Scenarios

| Current Count | Last Processed | Documents Since | Action |
|---------------|---------------|----------------|---------|
| 634 | 584 | 50 | ✅ Process 1 batch (50 docs) |
| 659 | 584 | 75 | ✅ Process 1 batch (50 docs), 25 remain |
| 734 | 584 | 150 | ✅ Process 3 batches (150 docs) |
| 614 | 584 | 30 | ⏳ Wait for 20 more documents |

## EventBridge Event Format

When a threshold is reached, this event is published:

```json
{
  "Source": "custom.mongodb.monitor",
  "DetailType": "Survey Results Threshold Reached",
  "Detail": {
    "current_count": 634,
    "last_processed_count": 584,
    "new_processed_count": 634,
    "documents_to_process": 50,
    "batches_ready": 1,
    "timestamp": "2025-01-19T10:30:00.000Z"
  }
}
```

## Connecting Processing Functions

To add automated processing when thresholds are reached:

1. Create a new Lambda function for your processing logic
2. Add it as a target to the `survey-processing-trigger` EventBridge rule
3. The function will receive the event data shown above

Example CloudFormation addition:

```yaml
ProcessingFunction:
  Type: AWS::Lambda::Function
  Properties:
    # Your processing function configuration

ProcessingTriggerTarget:
  Type: AWS::Events::Target
  Properties:
    Rule: !Ref ProcessingTriggerRule
    Arn: !GetAtt ProcessingFunction.Arn
    Id: ProcessingTarget
```

## Monitoring

### CloudWatch Logs
- Function logs: `/aws/lambda/mongodb-survey-monitor`
- Check for connection issues, threshold detections, errors

### DynamoDB Table
- Table: `survey-results-tracking`
- Monitor current counts and processing progress

### Useful Commands

```bash
# View recent Lambda logs
aws logs describe-log-groups --log-group-name-prefix '/aws/lambda/mongodb-survey-monitor'

# Check current tracking state
aws dynamodb get-item --table-name survey-results-tracking --key '{"id":{"S":"survey_results_count"}}'

# Manually invoke the monitor function
aws lambda invoke --function-name mongodb-survey-monitor response.json && cat response.json

# Disable monitoring temporarily
aws events disable-rule --name mongodb-monitor-schedule

# Re-enable monitoring
aws events enable-rule --name mongodb-monitor-schedule
```

## Configuration

### Environment Variables (in Lambda)
- `MONGODB_URI`: Connection string to MongoDB Atlas
- `TRACKING_TABLE`: DynamoDB table name for progress tracking

### Adjustable Parameters

#### Monitoring Frequency
Change the schedule in CloudFormation template:
```yaml
ScheduleExpression: 'rate(5 minutes)'  # Change to rate(10 minutes) for less frequent checks
```

#### Document Threshold
Modify the threshold logic in the Lambda function:
```python
threshold_crossed = documents_since_last_processed >= 50  # Change 50 to your desired threshold
```

## Cost Estimation

### Monthly Costs (Approximate)
- **Lambda**: $0.20 (8,640 invocations at 128MB, 5s avg runtime)
- **DynamoDB**: $0.25 (Pay-per-request, ~17,000 operations)
- **EventBridge**: $0.01 (8,640 events)
- **CloudWatch Logs**: $0.50 (log storage and queries)
- **S3**: $0.02 (artifact storage)

**Total: ~$1.00/month**

## Troubleshooting

### Common Issues

1. **MongoDB Connection Timeout**
   - Check VPC configuration if using VPC Lambda
   - Verify MongoDB URI and credentials
   - Check MongoDB Atlas IP whitelist

2. **DynamoDB Access Denied**
   - Verify Lambda IAM role has DynamoDB permissions
   - Check table name matches environment variable

3. **EventBridge Events Not Publishing**
   - Check Lambda logs for errors
   - Verify EventBridge permissions in IAM role

4. **High Costs**
   - Reduce monitoring frequency from 5 to 10+ minutes
   - Optimize Lambda memory allocation
   - Set up CloudWatch alarms for cost monitoring

### Reset Tracking State

To reset the processing counter (useful for testing):

```bash
aws dynamodb put-item --table-name survey-results-tracking --item '{
  "id": {"S": "survey_results_count"},
  "count": {"N": "584"},
  "last_processed_count": {"N": "584"},
  "timestamp": {"S": "'$(date -u +%Y-%m-%dT%H:%M:%S.000Z)'"}
}'
```

## Cleanup

To remove all resources:

```bash
aws cloudformation delete-stack --stack-name mongodb-survey-monitor
```

Note: Manual cleanup may be required for:
- S3 bucket contents (delete objects first)
- CloudWatch log groups
- Any additional processing functions you've added

## Next Steps

1. **Add Processing Functions**: Create Lambda functions to handle the threshold events
2. **Set Up Notifications**: Add SNS topics to notify when processing starts
3. **Monitoring Dashboard**: Create CloudWatch dashboard for visual monitoring
4. **Error Handling**: Add dead letter queues and retry logic for robustness
5. **Security**: Review and tighten IAM permissions for production use
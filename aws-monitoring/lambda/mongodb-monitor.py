import json
import os
import boto3
from pymongo import MongoClient
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
eventbridge = boto3.client('events')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Lambda function to monitor MongoDB survey.results collection
    Triggers processing when document count increases by 50
    """
    try:
        # MongoDB connection
        mongo_uri = os.environ['MONGODB_URI']
        client = MongoClient(mongo_uri)
        db = client.survey
        collection = db.results
        
        # Get current document count
        current_count = collection.count_documents({})
        logger.info(f"Current document count: {current_count}")
        
        # Get last processed count from DynamoDB
        table = dynamodb.Table(os.environ['TRACKING_TABLE'])
        
        try:
            response = table.get_item(Key={'id': 'survey_results_count'})
            if 'Item' in response:
                last_count = response['Item']['count']
                last_processed = response['Item']['last_processed_count']
            else:
                # First run - initialize tracking
                last_count = current_count
                last_processed = current_count
                table.put_item(Item={
                    'id': 'survey_results_count',
                    'count': current_count,
                    'last_processed_count': current_count,
                    'timestamp': datetime.utcnow().isoformat()
                })
                logger.info(f"Initialized tracking with count: {current_count}")
        except Exception as e:
            logger.error(f"Error accessing DynamoDB: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to access tracking table'})
            }
        
        # Check if we've crossed a 50-document threshold
        documents_since_last_processed = current_count - last_processed
        threshold_crossed = documents_since_last_processed >= 50
        
        logger.info(f"Last processed: {last_processed}, Current: {current_count}, Difference: {documents_since_last_processed}")
        
        if threshold_crossed:
            # Calculate how many 50-document batches we have
            batches_ready = documents_since_last_processed // 50
            new_processed_count = last_processed + (batches_ready * 50)
            
            # Publish custom event to EventBridge
            event_detail = {
                'current_count': current_count,
                'last_processed_count': last_processed,
                'new_processed_count': new_processed_count,
                'documents_to_process': batches_ready * 50,
                'batches_ready': batches_ready,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            eventbridge.put_events(
                Entries=[
                    {
                        'Source': 'custom.mongodb.monitor',
                        'DetailType': 'Survey Results Threshold Reached',
                        'Detail': json.dumps(event_detail),
                        'EventBusName': 'default'
                    }
                ]
            )
            
            # Update tracking table
            table.put_item(Item={
                'id': 'survey_results_count',
                'count': current_count,
                'last_processed_count': new_processed_count,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.info(f"Threshold reached! Published event for {batches_ready} batches ({batches_ready * 50} documents)")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'threshold_reached': True,
                    'current_count': current_count,
                    'documents_to_process': batches_ready * 50,
                    'batches_ready': batches_ready
                })
            }
        else:
            # Update current count in tracking table
            table.put_item(Item={
                'id': 'survey_results_count',
                'count': current_count,
                'last_processed_count': last_processed,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.info(f"No threshold reached. Need {50 - documents_since_last_processed} more documents.")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'threshold_reached': False,
                    'current_count': current_count,
                    'documents_until_next_batch': 50 - documents_since_last_processed
                })
            }
            
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        if 'client' in locals():
            client.close()
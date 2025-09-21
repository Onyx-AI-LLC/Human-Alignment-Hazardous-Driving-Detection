#!/usr/bin/env python3
"""
Test script for MongoDB Survey Monitor
Tests the Lambda function locally and checks AWS deployment
"""

import json
import boto3
from pymongo import MongoClient
import os
from datetime import datetime

# Configuration
MONGO_URI = "mongodb+srv://jonahmulcrone:1RFqimbiYxCpSlkU@hahdcluster.igin9.mongodb.net/survey?retryWrites=true&w=majority&appName=HAHDCluster"
STACK_NAME = "mongodb-survey-monitor"

def test_mongodb_connection():
    """Test MongoDB connection and get current count"""
    print("🔍 Testing MongoDB connection...")
    try:
        client = MongoClient(MONGO_URI)
        db = client.survey
        collection = db.results
        
        count = collection.count_documents({})
        print(f"✅ MongoDB connected successfully")
        print(f"📊 Current document count: {count}")
        
        # Get a sample document to verify structure
        sample = collection.find_one()
        if sample:
            print(f"📄 Sample document keys: {list(sample.keys())}")
        
        client.close()
        return count
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return None

def test_aws_resources():
    """Test AWS resources are deployed correctly"""
    print("\n🔍 Testing AWS resources...")
    
    try:
        # Check CloudFormation stack
        cf = boto3.client('cloudformation')
        stack = cf.describe_stacks(StackName=STACK_NAME)
        stack_status = stack['Stacks'][0]['StackStatus']
        print(f"✅ CloudFormation stack status: {stack_status}")
        
        # Get outputs
        outputs = {output['OutputKey']: output['OutputValue'] 
                  for output in stack['Stacks'][0].get('Outputs', [])}
        
        # Test Lambda function
        if 'MonitorFunctionArn' in outputs:
            lambda_client = boto3.client('lambda')
            function_name = outputs['MonitorFunctionArn'].split(':')[-1]
            
            response = lambda_client.get_function(FunctionName=function_name)
            print(f"✅ Lambda function exists: {function_name}")
            print(f"   Runtime: {response['Configuration']['Runtime']}")
            print(f"   Timeout: {response['Configuration']['Timeout']}s")
        
        # Test DynamoDB table
        if 'TrackingTableName' in outputs:
            dynamodb = boto3.resource('dynamodb')
            table_name = outputs['TrackingTableName']
            table = dynamodb.Table(table_name)
            
            # Check if table exists and is active
            table_status = table.table_status
            print(f"✅ DynamoDB table exists: {table_name}")
            print(f"   Status: {table_status}")
            
            # Check current tracking data
            try:
                response = table.get_item(Key={'id': 'survey_results_count'})
                if 'Item' in response:
                    item = response['Item']
                    print(f"   Last tracked count: {item.get('count', 'N/A')}")
                    print(f"   Last processed: {item.get('last_processed_count', 'N/A')}")
                    print(f"   Last update: {item.get('timestamp', 'N/A')}")
                else:
                    print("   No tracking data yet (first run will initialize)")
            except Exception as e:
                print(f"   Warning: Could not read tracking data: {e}")
        
        # Test EventBridge rule
        events = boto3.client('events')
        rules = events.list_rules(NamePrefix='mongodb-monitor')
        for rule in rules['Rules']:
            print(f"✅ EventBridge rule: {rule['Name']}")
            print(f"   State: {rule['State']}")
            print(f"   Schedule: {rule.get('ScheduleExpression', 'Event-driven')}")
        
        return True
    except Exception as e:
        print(f"❌ AWS resource test failed: {e}")
        return False

def test_lambda_invocation():
    """Test Lambda function invocation"""
    print("\n🔍 Testing Lambda function invocation...")
    
    try:
        lambda_client = boto3.client('lambda')
        function_name = "mongodb-survey-monitor"
        
        # Invoke the function
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        
        payload = json.loads(response['Payload'].read())
        print(f"✅ Lambda invocation successful")
        print(f"   Status Code: {response['StatusCode']}")
        print(f"   Response: {json.dumps(payload, indent=2)}")
        
        return True
    except Exception as e:
        print(f"❌ Lambda invocation failed: {e}")
        return False

def simulate_threshold_check(current_count):
    """Simulate the threshold logic locally"""
    print(f"\n🔍 Simulating threshold logic...")
    
    # Example scenarios
    scenarios = [
        {"last_processed": current_count - 30, "description": "20 documents away from threshold"},
        {"last_processed": current_count - 50, "description": "Exactly at threshold"},
        {"last_processed": current_count - 75, "description": "25 documents past threshold"},
        {"last_processed": current_count - 150, "description": "3 batches ready (150 documents)"},
    ]
    
    for scenario in scenarios:
        last_processed = scenario["last_processed"]
        documents_since = current_count - last_processed
        batches_ready = documents_since // 50
        threshold_crossed = documents_since >= 50
        
        print(f"\n📊 Scenario: {scenario['description']}")
        print(f"   Current count: {current_count}")
        print(f"   Last processed: {last_processed}")
        print(f"   Documents since: {documents_since}")
        print(f"   Threshold crossed: {threshold_crossed}")
        if threshold_crossed:
            print(f"   Batches ready: {batches_ready}")
            print(f"   Documents to process: {batches_ready * 50}")

def main():
    """Main test function"""
    print("🚀 MongoDB Survey Monitor Test Suite")
    print("=" * 50)
    
    # Test MongoDB
    current_count = test_mongodb_connection()
    if current_count is None:
        print("❌ Cannot continue without MongoDB connection")
        return
    
    # Test AWS resources
    aws_ok = test_aws_resources()
    if not aws_ok:
        print("⚠️  AWS resources may not be properly deployed")
        return
    
    # Test Lambda invocation
    lambda_ok = test_lambda_invocation()
    
    # Simulate threshold logic
    simulate_threshold_check(current_count)
    
    print("\n" + "=" * 50)
    if aws_ok and lambda_ok:
        print("✅ All tests passed! Your MongoDB monitor is working correctly.")
        print("\n📋 What happens next:")
        print("1. Lambda runs every 5 minutes automatically")
        print("2. When 50+ new documents are detected, an EventBridge event is published")
        print("3. You can create additional Lambda functions to handle these events")
        print("4. Monitor progress in CloudWatch logs and DynamoDB")
    else:
        print("❌ Some tests failed. Check the errors above.")

if __name__ == "__main__":
    main()
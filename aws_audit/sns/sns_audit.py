#!/usr/bin/env python3
"""
SNS Audit Tool

Collects information about all SNS topics in the account,
including their configuration, subscriptions, and tags.
The results are saved to an Excel spreadsheet for analysis.
"""
import boto3
import pandas as pd
from datetime import datetime, timezone, timedelta
import logging
from typing import List, Dict, Any, Optional
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SNSAuditor:
    def __init__(self):
        """Initialize AWS clients and data structures."""
        self.sns_client = boto3.client('sns')
        self.cloudwatch = boto3.client('cloudwatch')
        self.cf_client = boto3.client('cloudformation')
        self.topics = []
    
    def get_stack_name_from_arn(self, resource_arn: str) -> str:
        """
        Get the CloudFormation stack name from a resource ARN.
        Returns the stack name as a string or empty string if not found.
        """
        try:
            # First, try to get stack info from tags (fastest method)
            try:
                response = self.sns_client.list_tags_for_resource(
                    ResourceArn=resource_arn
                )
                tags = {tag['Key']: tag['Value'] for tag in response.get('Tags', [])}
                
                # Check for CloudFormation stack name tag
                stack_name = tags.get('aws:cloudformation:stack-name')
                if stack_name:
                    return stack_name
                    
            except Exception as e:
                logger.debug(f"Error getting tags for {resource_arn}: {str(e)}")
                
            # If tag lookup fails, try direct lookup (slower but more reliable)
            try:
                resource_id = resource_arn.split(':')[-1]
                response = self.cf_client.describe_stack_resources(
                    PhysicalResourceId=resource_id
                )
                
                if response.get('StackResources'):
                    return response['StackResources'][0]['StackName']
                    
            except self.cf_client.exceptions.ClientError as e:
                if 'does not exist' not in str(e):
                    logger.debug(f"Direct stack lookup failed for {resource_arn}: {str(e)}")
                    
        except Exception as e:
            logger.debug(f"Unexpected error getting stack info for {resource_arn}: {str(e)}")
            
        return ''  # Return empty string when not managed by CloudFormation
    
    def get_topic_metrics_30d(self, topic_arn: str) -> Dict[str, Any]:
        """Get metrics for an SNS topic for the last 30 days."""
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=30)
            topic_name = topic_arn.split(':')[-1]
            
            # Get message published count
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/SNS',
                MetricName='NumberOfMessagesPublished',
                Dimensions=[{'Name': 'TopicName', 'Value': topic_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,  # 1 day in seconds
                Statistics=['Sum'],
                Unit='Count'
            )
            
            # Calculate average messages per day
            datapoints = response.get('Datapoints', [])
            total_messages = sum(dp['Sum'] for dp in datapoints if 'Sum' in dp)
            avg_daily_messages = total_messages / 30 if datapoints else 0
            
            return {
                'AvgDailyMessages': round(avg_daily_messages, 2),
                'TotalMessages30d': int(total_messages)
            }
            
        except Exception as e:
            logger.error(f"Error getting metrics for topic {topic_arn}: {str(e)}")
            return {'AvgDailyMessages': 0, 'TotalMessages30d': 0}
    
    def get_topic_subscriptions(self, topic_arn: str) -> List[Dict[str, str]]:
        """Get all subscriptions for a topic."""
        try:
            response = self.sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
            return [
                {
                    'Protocol': sub['Protocol'],
                    'Endpoint': sub['Endpoint'],
                    'Status': sub['SubscriptionArn'] if sub['SubscriptionArn'] != 'PendingConfirmation' else 'PendingConfirmation'
                }
                for sub in response.get('Subscriptions', [])
            ]
        except Exception as e:
            logger.error(f"Error getting subscriptions for topic {topic_arn}: {str(e)}")
            return []
    
    def get_topic_attributes(self, topic_arn: str) -> Dict[str, Any]:
        """Get attributes for a topic."""
        try:
            response = self.sns_client.get_topic_attributes(TopicArn=topic_arn)
            return response.get('Attributes', {})
        except Exception as e:
            logger.error(f"Error getting attributes for topic {topic_arn}: {str(e)}")
            return {}
    
    def get_all_topics(self) -> List[Dict[str, Any]]:
        """Retrieve all SNS topics in the account."""
        logger.info("Fetching all SNS topics...")
        topics = []
        
        try:
            # Initialize pagination
            next_token = None
            
            while True:
                # Prepare parameters for list_topics
                params = {}
                if next_token:
                    params['NextToken'] = next_token
                
                # Get a page of topics
                response = self.sns_client.list_topics(**params)
                
                # Process each topic in the current page
                for topic in response.get('Topics', []):
                    try:
                        topic_arn = topic['TopicArn']
                        logger.info(f"Processing topic: {topic_arn}")
                        
                        # Get topic attributes
                        attributes = self.get_topic_attributes(topic_arn)
                        
                        # Get topic tags
                        tags_response = self.sns_client.list_tags_for_resource(ResourceArn=topic_arn)
                        tags = {tag['Key']: tag['Value'] for tag in tags_response.get('Tags', [])}
                        
                        # Get metrics
                        metrics = self.get_topic_metrics_30d(topic_arn)
                        
                        # Get subscriptions
                        subscriptions = self.get_topic_subscriptions(topic_arn)
                        
                        # Get stack name if managed by CloudFormation
                        stack_name = self.get_stack_name_from_arn(topic_arn)
                        
                        # Extract environment from name (common patterns)
                        topic_name = topic_arn.split(':')[-1]
                        name_parts = topic_name.lower().split('-')
                        environment = 'unknown'
                        env_indicators = ['dev', 'prod', 'staging', 'test', 'qa', 'uat', 'preprod']
                        
                        # First try to get from tags
                        environment = tags.get('environment', '').lower()
                        
                        # If not in tags, check name parts
                        if not environment or environment not in env_indicators:
                            for part in name_parts:
                                if part in env_indicators:
                                    environment = part
                                    break
                            else:
                                # If still not found, try common patterns in the name
                                if any(x in topic_name.lower() for x in ['-dev-', '.dev.']):
                                    environment = 'dev'
                                elif any(x in topic_name.lower() for x in ['-staging-', '-stage-', '.staging.', '.stage.']):
                                    environment = 'staging'
                                elif any(x in topic_name.lower() for x in ['-prod-', '-production-', '.prod.', '.production.']):
                                    environment = 'prod'
                        
                        # Prepare topic info
                        topic_info = {
                            'TopicArn': topic_arn,
                            'TopicName': topic_arn.split(':')[-1],
                            'Environment': environment,
                            'StackName': stack_name,
                            'DisplayName': attributes.get('DisplayName', ''),
                            'Owner': attributes.get('Owner', ''),
                            'SubscriptionsCount': len(subscriptions),
                            'Subscriptions': ', '.join([f"{sub['Protocol']}:{sub['Endpoint']}" for sub in subscriptions]),
                            'EffectiveDeliveryPolicy': attributes.get('EffectiveDeliveryPolicy', ''),
                            'Policy': attributes.get('Policy', ''),
                            'KmsMasterKeyId': attributes.get('KmsMasterKeyId', 'None'),
                            'FifoTopic': attributes.get('FifoTopic', 'false').lower() == 'true',
                            'ContentBasedDeduplication': attributes.get('ContentBasedDeduplication', 'false').lower() == 'true',
                            'Tags': ', '.join([f"{k}={v}" for k, v in tags.items()]),
                            **metrics
                        }
                        
                        topics.append(topic_info)
                        
                    except Exception as e:
                        logger.error(f"Error processing topic {topic.get('TopicArn', 'unknown')}: {str(e)}")
                        continue
                
                # Check if there are more topics to fetch
                next_token = response.get('NextToken')
                if not next_token:
                    break
                        
        except Exception as e:
            logger.error(f"Error listing SNS topics: {str(e)}")
            raise
            
        logger.info(f"Found {len(topics)} SNS topics")
        return topics

def save_to_excel(data: List[Dict[str, Any]], filename: str = 'sns_audit.xlsx') -> str:
    """
    Save the audit results to an Excel file.
    
    Args:
        data: List of dictionaries containing topic data
        filename: Name of the file to save to
        
    Returns:
        str: The absolute path to the saved file
    """
    if not data:
        logger.warning("No data to save to Excel")
        return ""
    
    # Define column mapping for better display
    column_mapping = {
        'TopicName': 'Topic Name',
        'Environment': 'Environment',
        'StackName': 'Stack Name',
        'DisplayName': 'Display Name',
        'AvgDailyMessages': 'Avg Daily Messages',
        'TotalMessages30d': 'Total Messages (30d)',
        'SubscriptionsCount': 'Subscription Count',
        'Subscriptions': 'Subscriptions',
        'KmsMasterKeyId': 'KMS Key',
        'FifoTopic': 'FIFO Topic',
        'ContentBasedDeduplication': 'Content-Based Deduplication',
        'TopicArn': 'Topic ARN',
        'Tags': 'Tags'
    }
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Reorder and rename columns according to mapping
    columns_order = [col for col in column_mapping.keys() if col in df.columns]
    df = df[columns_order]
    df = df.rename(columns=column_mapping)
    
    # Set output path to the parent directory (root folder)
    output_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(output_dir, filename)
    
    # Save to Excel using openpyxl
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='SNS Audit')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['SNS Audit']
        for idx, col in enumerate(df.columns):
            max_length = max((
                df[col].astype(str).map(len).max(),
                len(str(col))
            )) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
    
    logger.info(f"Audit results saved to: {output_path}")
    return output_path

def main() -> int:
    """Main function to run the SNS audit."""
    try:
        # Initialize auditor
        auditor = SNSAuditor()
        
        # Get all topics
        topics = auditor.get_all_topics()
        
        if not topics:
            logger.warning("No SNS topics found or an error occurred")
            return 1
            
        # Save to Excel
        output_path = save_to_excel(topics)
        logger.info(f"SNS audit completed successfully. Results saved to: {output_path}")
        return 0
        
    except Exception as e:
        logger.error(f"Error running SNS audit: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())

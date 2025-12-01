#!/usr/bin/env python3
"""
SQS Audit Tool

Collects information about all SQS queues in the account,
including their configuration, attributes, and tags.
The results are saved to an Excel spreadsheet for analysis.
"""
import boto3
import pandas as pd
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
import os
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQSAuditor:
    def __init__(self):
        """Initialize AWS clients and data structures."""
        self.sqs_client = boto3.client('sqs')
        self.cloudwatch = boto3.client('cloudwatch')
        self.cf_client = boto3.client('cloudformation')
        self.queues = []
    def get_stack_name_from_arn(self, resource_arn: str, queue_url: str = '') -> str:
        """
        Get the CloudFormation stack name from a resource ARN.
        Returns the stack name as a string or empty string if not found.
        """
        try:
            # First, try to get stack info from tags (fastest method)
            try:
                # Use provided queue_url or convert ARN to URL
                url = queue_url if queue_url else self._arn_to_url(resource_arn)
                response = self.sqs_client.list_queue_tags(QueueUrl=url)
                tags = response.get('Tags', {})
                
                # Check for CloudFormation stack name tag
                stack_name = tags.get('aws:cloudformation:stack-name')
                if stack_name:
                    return stack_name
                    
            except Exception as e:
                logger.debug(f"Error getting tags for {resource_arn}: {str(e)}")
                
            # Try CloudFormation API with different resource identifiers
            # CloudFormation might use queue name, URL, or ARN depending on how it was created
            # For SQS, the Queue URL is typically used as the PhysicalResourceId
            identifiers_to_try = [
                queue_url if queue_url else self._arn_to_url(resource_arn),  # Queue URL (most common)
                resource_arn.split(':')[-1],  # Queue name
                resource_arn  # Full ARN
            ]
            
            for resource_id in identifiers_to_try:
                try:
                    response = self.cf_client.describe_stack_resources(
                        PhysicalResourceId=resource_id
                    )
                    
                    if response.get('StackResources'):
                        return response['StackResources'][0]['StackName']
                        
                except self.cf_client.exceptions.ClientError as e:
                    if 'does not exist' in str(e):
                        continue  # Try next identifier
                    logger.debug(f"CloudFormation lookup failed for {resource_id}: {str(e)}")
                except Exception as e:
                    logger.debug(f"Error trying identifier {resource_id}: {str(e)}")
                    
        except Exception as e:
            logger.debug(f"Unexpected error getting stack info for {resource_arn}: {str(e)}")
            
        return ''  # Return empty string when not managed by CloudFormation
    
    def _arn_to_url(self, queue_arn: str) -> str:
        """Convert a queue ARN to a queue URL."""
        # ARN format: arn:aws:sqs:region:account-id:queue-name
        parts = queue_arn.split(':')
        if len(parts) >= 6:
            region = parts[3]
            account_id = parts[4]
            queue_name = parts[5]
            return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
        return ""
    
        
    def get_message_count_30d(self, queue_name: str) -> int:
        """Get the number of messages received in the last 30 days."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=30)
            
            # Get data points for each hour in the 30-day period
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/SQS',
                MetricName='NumberOfMessagesReceived',
                Dimensions=[
                    {
                        'Name': 'QueueName',
                        'Value': queue_name
                    },
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour in seconds
                Statistics=['Sum'],
                Unit='Count'
            )
            
            if 'Datapoints' in response and response['Datapoints']:
                # Sum up all the hourly data points
                total_messages = sum(int(point['Sum']) for point in response['Datapoints'])
                return total_messages
            return 0
        except Exception as e:
            logger.warning(f"Could not get message count for {queue_name}: {str(e)}")
            return -1  # -1 indicates an error occurred
        
    def get_all_queues(self) -> List[Dict[str, Any]]:
        """Retrieve all SQS queues in the account."""
        logger.info("Fetching all SQS queues...")
        queues = []
        
        try:
            response = self.sqs_client.list_queues()
            queue_urls = response.get('QueueUrls', [])
            
            for queue_url in queue_urls:
                try:
                    # Get queue attributes
                    attributes = self.get_queue_attributes(queue_url)
                    # Get queue tags
                    tags = self.get_queue_tags(queue_url)
                    
                    # Get queue name for metrics
                    queue_name = queue_url.split('/')[-1]
                    # Get message count for last 30 days
                    attributes['MessagesReceived30d'] = self.get_message_count_30d(queue_name)
                    
                    # Extract queue name from URL
                    queue_name = queue_url.split('/')[-1]
                    
                    # Get stack name from CloudFormation
                    queue_arn = attributes.get('QueueArn', '')
                    stack_name = self.get_stack_name_from_arn(queue_arn, queue_url) if queue_arn else ''
                    
                    # Prepare queue info
                    queue_info = {
                        'QueueName': queue_name,
                        'QueueUrl': queue_url,
                        'Region': self.get_region_from_url(queue_url),
                        'StackName': stack_name,
                        **attributes,
                        'Tags': ', '.join([f"{k}={v}" for k, v in tags.items()]) if tags else 'None'
                    }
                    
                    queues.append(queue_info)
                    logger.info(f"Processed queue: {queue_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing queue {queue_url}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error listing SQS queues: {str(e)}")
            
        logger.info(f"Found {len(queues)} SQS queues")
        return queues
    
    def get_queue_attributes(self, queue_url: str) -> Dict[str, Any]:
        """Get attributes for a specific SQS queue."""
        # Common attributes for all queue types
        standard_attributes = [
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed',
            'CreatedTimestamp',
            'DelaySeconds',
            'LastModifiedTimestamp',
            'MaximumMessageSize',
            'MessageRetentionPeriod',
            'Policy',
            'QueueArn',
            'ReceiveMessageWaitTimeSeconds',
            'RedrivePolicy',
            'VisibilityTimeout',
            'KmsMasterKeyId',
            'KmsDataKeyReusePeriodSeconds',
            'SqsManagedSseEnabled'
        ]
        
        # FIFO-specific attributes
        fifo_attributes = [
            'FifoQueue',
            'ContentBasedDeduplication'
        ]
        
        try:
            # First get standard attributes
            response = self.sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=standard_attributes
            )
            
            # Check if this is a FIFO queue by looking at the queue URL
            is_fifo = queue_url.endswith('.fifo')
            
            # If it's a FIFO queue, get the FIFO-specific attributes
            if is_fifo:
                try:
                    fifo_response = self.sqs_client.get_queue_attributes(
                        QueueUrl=queue_url,
                        AttributeNames=fifo_attributes
                    )
                    response['Attributes'].update(fifo_response['Attributes'])
                except Exception as fifo_error:
                    logger.warning(f"Could not get FIFO attributes for queue {queue_url}: {str(fifo_error)}")
                    # Add default FIFO attributes
                    response['Attributes'].update({
                        'FifoQueue': 'true',
                        'ContentBasedDeduplication': 'false'
                    })
            
            # Convert timestamp to readable format
            attrs = response.get('Attributes', {})
            if 'CreatedTimestamp' in attrs:
                attrs['CreatedTimestamp'] = self._format_timestamp(attrs['CreatedTimestamp'])
            if 'LastModifiedTimestamp' in attrs:
                attrs['LastModifiedTimestamp'] = self._format_timestamp(attrs['LastModifiedTimestamp'])
                
            # Convert seconds to minutes/hours/days for better readability
            if 'MessageRetentionPeriod' in attrs:
                attrs['MessageRetentionPeriod'] = self._seconds_to_readable(attrs['MessageRetentionPeriod'])
            if 'VisibilityTimeout' in attrs:
                attrs['VisibilityTimeout'] = self._seconds_to_readable(attrs['VisibilityTimeout'])
            if 'DelaySeconds' in attrs:
                attrs['DelaySeconds'] = self._seconds_to_readable(attrs['DelaySeconds'])
                
            return attrs
            
        except Exception as e:
            logger.warning(f"Could not get attributes for queue {queue_url}: {str(e)}")
            return {}
    
    def get_queue_tags(self, queue_url: str) -> Dict[str, str]:
        """Get tags for a specific SQS queue."""
        try:
            response = self.sqs_client.list_queue_tags(QueueUrl=queue_url)
            return response.get('Tags', {})
        except Exception as e:
            logger.warning(f"Could not get tags for queue {queue_url}: {str(e)}")
            return {}
    
    def _format_timestamp(self, timestamp_str: str) -> str:
        """Convert epoch timestamp to human-readable format."""
        try:
            return datetime.fromtimestamp(int(timestamp_str)).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            return timestamp_str
    
    def _seconds_to_readable(self, seconds_str: str) -> str:
        """Convert seconds to a more readable format (e.g., '5 minutes')."""
        try:
            seconds = int(seconds_str)
            minutes, seconds = divmod(seconds, 60)
            hours, minutes = divmod(minutes, 60)
            days, hours = divmod(hours, 24)
            
            parts = []
            if days > 0:
                parts.append(f"{days} day{'s' if days > 1 else ''}")
            if hours > 0:
                parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
            if minutes > 0 and days == 0:  # Only show minutes if less than a day
                parts.append(f"{minutes} minute{'s' if minutes > 1 else ''}")
            if seconds > 0 and hours == 0 and days == 0:  # Only show seconds if less than an hour
                parts.append(f"{seconds} second{'s' if seconds > 1 else ''}")
                
            return ' '.join(parts) if parts else '0 seconds'
        except (ValueError, TypeError):
            return seconds_str
    
    def get_region_from_url(self, queue_url: str) -> str:
        """Extract region from queue URL."""
        # Example URL: https://sqs.region.amazonaws.com/account-id/queue-name
        try:
            return queue_url.split('.')[1]
        except (IndexError, AttributeError):
            return 'unknown'
    
    def audit_queues(self) -> List[Dict[str, Any]]:
        """Main method to audit all SQS queues."""
        return self.get_all_queues()


def save_to_excel(data: List[Dict[str, Any]], filename: str = 'sqs_audit.xlsx') -> str:
    """Save the audit results to an Excel file.
    
    Args:
        data: List of dictionaries containing queue data
        filename: Name of the file to save to
        
    Returns:
        str: The absolute path to the saved file
    """
    if not data:
        logger.warning("No data to save")
        return filename
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Add Environment column
    def get_environment(queue_name):
        if not isinstance(queue_name, str):
            return 'unknown'
        parts = queue_name.split('-')
        if len(parts) >= 2 and parts[-1] in ['dev', 'prod', 'staging']:
            return parts[-1]
        return 'unknown'
    
    df['Environment'] = df['QueueName'].apply(get_environment)
    
    # StackName is already in the data from get_all_queues()
    # Ensure it exists in case of any missing values
    if 'StackName' not in df.columns:
        df['StackName'] = ''
    
    # Define the column order and their display names
    column_mapping = {
        'QueueName': 'Queue Name',
        'Environment': 'Environment',
        'StackName': 'Stack Name',
        'MessagesReceived30d': 'Messages (30d)',
        'ApproximateNumberOfMessages': 'Messages Available',
        'ApproximateNumberOfMessagesNotVisible': 'Messages In Flight',
        'ApproximateNumberOfMessagesDelayed': 'Messages Delayed',
        'CreatedTimestamp': 'Created',
        'LastModifiedTimestamp': 'Last Modified',
        'QueueArn': 'ARN',
        'MessageRetentionPeriod': 'Message Retention (seconds)',
        'VisibilityTimeout': 'Visibility Timeout (seconds)',
        'DelaySeconds': 'Delivery Delay (seconds)',
        'FifoQueue': 'FIFO Queue',
        'ContentBasedDeduplication': 'Content-Based Deduplication',
        'KmsMasterKeyId': 'KMS Master Key ID',
        'SqsManagedSseEnabled': 'SQS-Managed SSE Enabled',
        'QueueUrl': 'Queue URL',
        'MaximumMessageSize': 'Max Message Size (bytes)',
        'Policy': 'Queue Policy',
        'ReceiveMessageWaitTimeSeconds': 'Receive Wait Time (seconds)',
        'RedrivePolicy': 'Redrive Policy',
        'KmsDataKeyReusePeriodSeconds': 'KMS Data Key Reuse Period (seconds)',
        'Tags': 'Tags'
    }
    
    # Create ordered list of columns that exist in the DataFrame
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    
    # Add any extra columns that weren't in our mapping
    extra_columns = [col for col in df.columns if col not in column_mapping]
    
    # Reorder and rename columns
    df = df[existing_columns + extra_columns]
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Save to Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='SQS Audit')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['SQS Audit']
        for idx, col in enumerate(df.columns):
            max_length = max((
                df[col].astype(str).map(len).max(),
                len(str(col))
            )) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
    
    abs_path = os.path.abspath(filename)
    logger.info(f"Audit results saved to {abs_path}")
    return abs_path


def main():
    """Main function to run the SQS audit."""
    try:
        logger.info("Starting SQS audit...")
        auditor = SQSAuditor()
        results = auditor.audit_queues()
        
        if results:
            output_file = 'sqs_audit.xlsx'
            saved_file = save_to_excel(results, output_file)
            print(f"\nAudit completed successfully! Results saved to: {os.path.abspath(saved_file)}")
            print(f"Total SQS queues audited: {len(results)}")
        else:
            print("No SQS queues found or an error occurred during the audit.")
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

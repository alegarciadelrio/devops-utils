#!/usr/bin/env python3
"""
S3 Audit Tool

Collects information about all S3 buckets in the account,
including their configuration, encryption, and tags.
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

class S3Auditor:
    def __init__(self):
        """Initialize AWS clients and data structures."""
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.cloudwatch = boto3.client('cloudwatch')
        self.cf_client = boto3.client('cloudformation')
        self.buckets = []
    
    def get_stack_name_from_bucket(self, bucket_name: str) -> str:
        """
        Try to find if the bucket is managed by CloudFormation.
        Returns the stack name or empty string if not found.
        """
        try:
            # Check bucket tags for CloudFormation stack info
            try:
                tags = self.s3_client.get_bucket_tagging(Bucket=bucket_name)
                for tag in tags.get('TagSet', []):
                    if tag['Key'] == 'aws:cloudformation:stack-name':
                        return tag['Value']
            except Exception:
                # No tags or no permission to read tags
                pass
                
            # Try to find the bucket in CloudFormation resources
            response = self.cf_client.list_stack_resources()
            for resource in response.get('StackResourceSummaries', []):
                if (resource['ResourceType'] == 'AWS::S3::Bucket' and 
                    resource.get('PhysicalResourceId') == bucket_name):
                    return resource['StackName']
                    
            # Check if bucket name follows a naming convention with stack name
            if '-' in bucket_name:
                possible_stack_name = '-'.join(bucket_name.split('-')[:-1])
                try:
                    self.cf_client.describe_stacks(StackName=possible_stack_name)
                    return possible_stack_name
                except Exception:
                    pass
                    
        except Exception as e:
            logger.debug(f"Error getting stack info for bucket {bucket_name}: {str(e)}")
            
        return ''  # Return empty string when not managed by CloudFormation
    
    def get_bucket_metrics_30d(self, bucket_name: str) -> Dict[str, Any]:
        """Get storage and request metrics for the bucket for the last 30 days."""
        metrics = {
            'BucketSizeBytes': 0,
            'NumberOfObjects': 0,
            'AllRequests': 0,
            'GetRequests': 0,
            'PutRequests': 0,
            'DeleteRequests': 0,
            'HeadRequests': 0,
            'PostRequests': 0,
            'ListRequests': 0,
            '4xxErrors': 0,
            '5xxErrors': 0,
            'BytesDownloaded': 0,
            'BytesUploaded': 0
        }
        
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=30)
            
            # Get bucket size metrics
            try:
                size_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='BucketSizeBytes',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket_name},
                        {'Name': 'StorageType', 'Value': 'StandardStorage'}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,  # 1 day in seconds
                    Statistics=['Average'],
                    Unit='Bytes'
                )
                if size_response.get('Datapoints'):
                    metrics['BucketSizeBytes'] = int(max(dp['Average'] for dp in size_response['Datapoints']))
            except Exception as e:
                logger.debug(f"Error getting size metrics for bucket {bucket_name}: {str(e)}")
            
            # Get number of objects
            try:
                obj_response = self.cloudwatch.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='NumberOfObjects',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket_name},
                        {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,
                    Statistics=['Average'],
                    Unit='Count'
                )
                if obj_response.get('Datapoints'):
                    metrics['NumberOfObjects'] = int(max(dp['Average'] for dp in obj_response['Datapoints']))
            except Exception as e:
                logger.debug(f"Error getting object count for bucket {bucket_name}: {str(e)}")
            
            # Get request metrics
            req_metrics = [
                'AllRequests', 'GetRequests', 'PutRequests', 'DeleteRequests',
                'HeadRequests', 'PostRequests', 'ListRequests', '4xxErrors', '5xxErrors'
            ]
            
            for metric in req_metrics:
                try:
                    req_response = self.cloudwatch.get_metric_statistics(
                        Namespace='AWS/S3',
                        MetricName=metric,
                        Dimensions=[{'Name': 'BucketName', 'Value': bucket_name}],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Sum'],
                        Unit='Count'
                    )
                    if req_response.get('Datapoints'):
                        metrics[metric] = int(sum(dp['Sum'] for dp in req_response['Datapoints']))
                except Exception as e:
                    logger.debug(f"Error getting {metric} for bucket {bucket_name}: {str(e)}")
            
            # Get data transfer metrics
            transfer_metrics = {
                'BytesDownloaded': 'Bytes',
                'BytesUploaded': 'Bytes'
            }
            
            for metric_name, unit in transfer_metrics.items():
                try:
                    transfer_response = self.cloudwatch.get_metric_statistics(
                        Namespace='AWS/S3',
                        MetricName=metric_name,
                        Dimensions=[{'Name': 'BucketName', 'Value': bucket_name}],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=86400,
                        Statistics=['Sum'],
                        Unit=unit
                    )
                    if transfer_response.get('Datapoints'):
                        metrics[metric_name] = int(sum(dp['Sum'] for dp in transfer_response['Datapoints']))
                except Exception as e:
                    logger.debug(f"Error getting {metric_name} for bucket {bucket_name}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Unexpected error getting metrics for bucket {bucket_name}: {str(e)}")
            
        return metrics
    
    def get_bucket_encryption(self, bucket_name: str) -> str:
        """Get the encryption configuration of the bucket."""
        try:
            response = self.s3_client.get_bucket_encryption(Bucket=bucket_name)
            rules = response.get('ServerSideEncryptionConfiguration', {}).get('Rules', [])
            if rules:
                return rules[0].get('ApplyServerSideEncryptionByDefault', {}).get('SSEAlgorithm', 'None')
        except Exception as e:
            if 'ServerSideEncryptionConfigurationNotFoundError' in str(e):
                return 'None'
            logger.debug(f"Error getting encryption for bucket {bucket_name}: {str(e)}")
        return 'Unknown'
    
    def get_bucket_versioning(self, bucket_name: str) -> str:
        """Get the versioning status of the bucket."""
        try:
            response = self.s3_client.get_bucket_versioning(Bucket=bucket_name)
            return response.get('Status', 'Disabled')
        except Exception as e:
            logger.debug(f"Error getting versioning for bucket {bucket_name}: {str(e)}")
            return 'Error'
    
    def get_bucket_logging(self, bucket_name: str) -> str:
        """Check if logging is enabled for the bucket."""
        try:
            response = self.s3_client.get_bucket_logging(Bucket=bucket_name)
            if 'LoggingEnabled' in response:
                return 'Enabled'
        except Exception as e:
            logger.debug(f"Error getting logging status for bucket {bucket_name}: {str(e)}")
        return 'Disabled'
    
    def get_bucket_tags(self, bucket_name: str) -> Dict[str, str]:
        """Get tags for the bucket."""
        try:
            response = self.s3_client.get_bucket_tagging(Bucket=bucket_name)
            return {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
        except Exception as e:
            # No tags is a common case, don't log as error
            if 'NoSuchTagSet' not in str(e):
                logger.debug(f"Error getting tags for bucket {bucket_name}: {str(e)}")
            return {}
    
    def get_bucket_public_access(self, bucket_name: str) -> str:
        """Check if the bucket has public access."""
        try:
            # Check bucket policy for public access
            policy = self.s3_client.get_bucket_policy_status(Bucket=bucket_name)
            if policy.get('PolicyStatus', {}).get('IsPublic'):
                return 'Public'
                
            # Check ACL for public access
            acl = self.s3_client.get_bucket_acl(Bucket=bucket_name)
            for grant in acl.get('Grants', []):
                grantee = grant.get('Grantee', {})
                if 'URI' in grantee and 'AllUsers' in grantee['URI']:
                    return 'Public'
                    
            return 'Private'
            
        except Exception as e:
            # If we can't check, assume private for safety
            logger.debug(f"Error checking public access for bucket {bucket_name}: {str(e)}")
            return 'Unknown'
    
    def get_environment_from_name(self, bucket_name: str, tags: Dict[str, str]) -> str:
        """Extract environment from bucket name and tags."""
        # First try to get from tags
        env = tags.get('environment') or tags.get('Environment')
        if env:
            return env.lower()
            
        # Common environment indicators
        env_indicators = ['dev', 'prod', 'staging', 'test', 'qa', 'uat', 'preprod']
        
        # Check name parts
        name_parts = bucket_name.lower().split('-')
        for part in name_parts:
            if part in env_indicators:
                return part
                
        # Check for common patterns
        if any(x in bucket_name.lower() for x in ['-dev-', '.dev.', '-development']):
            return 'dev'
        elif any(x in bucket_name.lower() for x in ['-staging-', '-stage-', '.staging.', '.stage.']):
            return 'staging'
        elif any(x in bucket_name.lower() for x in ['-prod-', '-production-', '.prod.', '.production.']):
            return 'prod'
            
        return 'unknown'
    
    def get_all_buckets(self) -> List[Dict[str, Any]]:
        """Retrieve all S3 buckets in the account with detailed information."""
        logger.info("Fetching all S3 buckets...")
        buckets = []
        
        try:
            # Get list of all buckets
            response = self.s3_client.list_buckets()
            
            for bucket in response.get('Buckets', []):
                try:
                    bucket_name = bucket['Name']
                    logger.info(f"Processing bucket: {bucket_name}")
                    
                    # Skip AWS Logs buckets to avoid permission issues
                    if bucket_name.startswith('aws-') and ('logs' in bucket_name or 'logging' in bucket_name):
                        continue
                    
                    # Get bucket details
                    location = self.s3_client.get_bucket_location(Bucket=bucket_name).get('LocationConstraint', 'us-east-1')
                    if not location:  # us-east-1 returns None
                        location = 'us-east-1'
                        
                    # Get tags
                    tags = self.get_bucket_tags(bucket_name)
                    
                    # Get stack name if managed by CloudFormation
                    stack_name = self.get_stack_name_from_bucket(bucket_name)
                    
                    # Get environment
                    environment = self.get_environment_from_name(bucket_name, tags)
                    
                    # Get additional bucket properties
                    try:
                        versioning = self.get_bucket_versioning(bucket_name)
                        encryption = self.get_bucket_encryption(bucket_name)
                        logging_status = self.get_bucket_logging(bucket_name)
                        public_access = self.get_bucket_public_access(bucket_name)
                    except Exception as e:
                        logger.warning(f"Error getting properties for bucket {bucket_name}: {str(e)}")
                        versioning = 'Error'
                        encryption = 'Error'
                        logging_status = 'Error'
                        public_access = 'Error'
                    
                    # Get metrics (this can be slow, so we'll do it last)
                    metrics = self.get_bucket_metrics_30d(bucket_name)
                    
                    # Generate the ARN for the bucket
                    # S3 ARN format: arn:aws:s3:::<bucket_name>
                    bucket_arn = f'arn:aws:s3:::{bucket_name}'
                    
                    # Prepare bucket info
                    bucket_info = {
                        'BucketName': bucket_name,
                        'Arn': bucket_arn,
                        'CreationDate': bucket.get('CreationDate', '').strftime('%Y-%m-%d %H:%M:%S') if bucket.get('CreationDate') else '',
                        'Region': location,
                        'Environment': environment,
                        'StackName': stack_name,
                        'Versioning': versioning,
                        'Encryption': encryption,
                        'Logging': logging_status,
                        'PublicAccess': public_access,
                        'SizeBytes': metrics['BucketSizeBytes'],
                        'ObjectCount': metrics['NumberOfObjects'],
                        'TotalRequests': metrics['AllRequests'],
                        'GetRequests': metrics['GetRequests'],
                        'PutRequests': metrics['PutRequests'],
                        'Tags': ', '.join([f"{k}={v}" for k, v in tags.items()])
                    }
                    
                    buckets.append(bucket_info)
                    
                except Exception as e:
                    logger.error(f"Error processing bucket {bucket.get('Name', 'unknown')}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error listing S3 buckets: {str(e)}")
            raise
            
        logger.info(f"Found {len(buckets)} S3 buckets")
        return buckets

def save_to_excel(data: List[Dict[str, Any]], filename: str = 's3_audit.xlsx') -> str:
    """
    Save the audit results to an Excel file.
    
    Args:
        data: List of dictionaries containing bucket data
        filename: Name of the file to save to
        
    Returns:
        str: The absolute path to the saved file
    """
    if not data:
        logger.warning("No data to save to Excel")
        return ""
    
    # Define column mapping for better display
    column_mapping = {
        'BucketName': 'Bucket Name',
        'Environment': 'Environment',
        'StackName': 'Stack Name',
        'Region': 'Region',
        'TotalRequests': 'Total Requests (30d)',
        'GetRequests': 'GET Requests (30d)',
        'PutRequests': 'PUT Requests (30d)',
        'SizeBytes': 'Size (Bytes)',
        'ObjectCount': 'Object Count',
        'Versioning': 'Versioning',
        'Encryption': 'Encryption',
        'Logging': 'Logging',
        'PublicAccess': 'Public Access',
        'CreationDate': 'Creation Date',
        'Arn': 'ARN',
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
        df.to_excel(writer, index=False, sheet_name='S3 Audit')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['S3 Audit']
        for idx, col in enumerate(df.columns):
            max_length = max((
                df[col].astype(str).map(len).max(),
                len(str(col))
            )) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
    
    logger.info(f"Audit results saved to: {output_path}")
    return output_path

def main() -> int:
    """Main function to run the S3 audit."""
    try:
        # Initialize auditor
        auditor = S3Auditor()
        
        # Get all buckets
        buckets = auditor.get_all_buckets()
        
        if not buckets:
            logger.warning("No S3 buckets found or an error occurred")
            return 1
            
        # Save to Excel
        output_path = save_to_excel(buckets)
        logger.info(f"S3 audit completed successfully. Results saved to: {output_path}")
        return 0
        
    except Exception as e:
        logger.error(f"Error running S3 audit: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())

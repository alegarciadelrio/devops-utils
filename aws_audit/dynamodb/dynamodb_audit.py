#!/usr/bin/env python3
"""
DynamoDB Audit Tool

Collects information about all DynamoDB tables in the account,
including their configuration, metrics, and tags.
The results are saved to an Excel spreadsheet for analysis.
"""
import boto3
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
import logging
from typing import List, Dict, Any, Optional
from openpyxl.styles import PatternFill, Font, Alignment

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DynamoDBAuditor:
    def __init__(self):
        """Initialize AWS clients and data structures."""
        self.dynamodb = boto3.client('dynamodb')
        self.cloudwatch = boto3.client('cloudwatch')
        self.cloudformation = boto3.client('cloudformation')
        self.resourcegroupstagging = boto3.client('resourcegroupstaggingapi')
        self.tables = []
        
    def get_stack_name_from_arn(self, resource_arn: str) -> str:
        """
        Get the CloudFormation stack name from a resource ARN.
        Returns the stack name as a string or None if not found.
        """
        try:
            # First, try to get stack info from tags (fastest method)
            try:
                response = self.dynamodb.list_tags_of_resource(ResourceArn=resource_arn)
                tags = {tag['Key']: tag['Value'] for tag in response.get('Tags', [])}
                
                # Check for CloudFormation stack name tag
                stack_name = tags.get('aws:cloudformation:stack-name')
                if stack_name:
                    return stack_name
                    
            except Exception as e:
                logger.debug(f"Error getting tags for {resource_arn}: {str(e)}")
                
            # If tag lookup fails, try direct lookup (slower but more reliable)
            try:
                # Extract the table name from the ARN
                table_name = resource_arn.split('/')[-1]
                
                # Search for the table in CloudFormation resources
                response = self.cloudformation.describe_stack_resources(PhysicalResourceId=table_name)
                
                if response.get('StackResources'):
                    return response['StackResources'][0]['StackName']
                    
            except self.cloudformation.exceptions.ClientError as e:
                if 'does not exist' not in str(e):
                    logger.debug(f"Direct stack lookup failed for {resource_arn}: {str(e)}")
                    
        except Exception as e:
            logger.debug(f"Unexpected error getting stack info for {resource_arn}: {str(e)}")
            
        return ''  # Return empty string when not managed by CloudFormation
        
    def get_table_metrics_30d(self, table_name: str) -> Dict[str, Any]:
        """Get metrics for a DynamoDB table for the last 30 days."""
        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=30)
            
            # Get read/write capacity units
            metrics = {}
            
            # Get read capacity units
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/DynamoDB',
                MetricName='ConsumedReadCapacityUnits',
                Dimensions=[
                    {
                        'Name': 'TableName',
                        'Value': table_name
                    },
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600 * 24,  # 1 day in seconds
                Statistics=['Sum'],
                Unit='Count'
            )
            
            metrics['AvgDailyReadUnits'] = sum(
                point['Sum'] for point in response.get('Datapoints', [])
            ) / 30 if response.get('Datapoints') else 0
            
            # Get write capacity units
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/DynamoDB',
                MetricName='ConsumedWriteCapacityUnits',
                Dimensions=[
                    {
                        'Name': 'TableName',
                        'Value': table_name
                    },
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600 * 24,  # 1 day in seconds
                Statistics=['Sum'],
                Unit='Count'
            )
            
            metrics['AvgDailyWriteUnits'] = sum(
                point['Sum'] for point in response.get('Datapoints', [])
            ) / 30 if response.get('Datapoints') else 0
            
            # Get item count (approximate)
            try:
                response = self.dynamodb.describe_table(TableName=table_name)
                metrics['ItemCount'] = response['Table'].get('ItemCount', 0)
                metrics['TableSizeBytes'] = response['Table'].get('TableSizeBytes', 0)
            except Exception as e:
                logger.warning(f"Could not get table size for {table_name}: {str(e)}")
                metrics['ItemCount'] = -1
                metrics['TableSizeBytes'] = -1
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Could not get metrics for table {table_name}: {str(e)}")
            return {
                'AvgDailyReadUnits': -1,
                'AvgDailyWriteUnits': -1,
                'ItemCount': -1,
                'TableSizeBytes': -1
            }
    
    def get_table_tags(self, table_arn: str) -> Dict[str, str]:
        """Get tags for a specific DynamoDB table."""
        try:
            response = self.dynamodb.list_tags_of_resource(ResourceArn=table_arn)
            return {tag['Key']: tag['Value'] for tag in response.get('Tags', [])}
        except Exception as e:
            logger.warning(f"Could not get tags for table {table_arn}: {str(e)}")
            return {}
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Retrieve all DynamoDB tables in the account."""
        logger.info("Fetching all DynamoDB tables...")
        tables = []
        
        try:
            paginator = self.dynamodb.get_paginator('list_tables')
            pages = paginator.paginate()  # Fixed: Changed pagose() to paginate()
            
            for page in pages:
                for table_name in page.get('TableNames', []):
                    try:
                        # Get table details
                        response = self.dynamodb.describe_table(TableName=table_name)
                        table = response['Table']
                        
                        # Get metrics
                        metrics = self.get_table_metrics_30d(table_name)
                        
                        # Get tags
                        tags = self.get_table_tags(table['TableArn'])
                        
                        # Extract environment from name (common patterns)
                        name_parts = table_name.lower().split('-')
                        environment = 'unknown'
                        env_indicators = ['dev', 'prod', 'staging', 'test', 'qa', 'uat', 'preprod']
                        
                        # Check for environment in name parts
                        for part in name_parts:
                            if part in env_indicators:
                                environment = part
                                break
                        
                        # Get stack information
                        stack_name = self.get_stack_name_from_arn(table['TableArn'])
                        
                        # Prepare table info
                        table_info = {
                            'TableName': table_name,
                            'Environment': environment.upper(),
                            'StackName': stack_name,
                            'Status': table.get('TableStatus', 'UNKNOWN'),
                            'ARN': table.get('TableArn', ''),
                            'CreationDateTime': table.get('CreationDateTime', '').strftime('%Y-%m-%d %H:%M:%S') 
                                          if 'CreationDateTime' in table else '',
                            'ItemCount': metrics['ItemCount'],
                            'TableSizeBytes': metrics['TableSizeBytes'],
                            'AvgDailyReadUnits': metrics['AvgDailyReadUnits'],
                            'AvgDailyWriteUnits': metrics['AvgDailyWriteUnits'],
                            'BillingMode': table.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED'),
                            'ProvisionedReadCapacity': table.get('ProvisionedThroughput', {}).get('ReadCapacityUnits', 'N/A'),
                            'ProvisionedWriteCapacity': table.get('ProvisionedThroughput', {}).get('WriteCapacityUnits', 'N/A'),
                            'EncryptionType': table.get('SSEDescription', {}).get('SSEType', 'DEFAULT'),
                            'StreamEnabled': table.get('StreamSpecification', {}).get('StreamEnabled', False),
                            'Tags': ', '.join([f"{k}={v}" for k, v in tags.items()]) if tags else 'None'
                        }
                        
                        tables.append(table_info)
                        logger.info(f"Processed table: {table_name}")
                        
                    except Exception as e:
                        logger.error(f"Error processing table {table_name}: {str(e)}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error listing DynamoDB tables: {str(e)}")
            
        return tables
        
        logger.info(f"Found {len(tables)} DynamoDB tables")

def save_to_excel(data: List[Dict[str, Any]], filename: str = 'dynamodb_audit.xlsx') -> str:
    """
    Save the audit results to an Excel file.
    
    Args:
        data: List of dictionaries containing table data
        filename: Name of the file to save to
        
    Returns:
        str: The absolute path to the saved file
    """
    if not data:
        logger.warning("No data to save to Excel")
        return ""
    
    # Define column mapping for better display
    column_mapping = {
        'TableName': 'Table Name',
        'Environment': 'Environment',
        'StackName': 'Stack Name',
        'Status': 'Status',
        'ItemCount': 'Item Count',
        'TableSizeBytes': 'Table Size (Bytes)',
        'AvgDailyReadUnits': 'Avg Daily Read (Units)',
        'AvgDailyWriteUnits': 'Avg Daily Write (Units)',
        'BillingMode': 'Billing Mode',
        'ProvisionedReadCapacity': 'Provisioned Read Capacity',
        'ProvisionedWriteCapacity': 'Provisioned Write Capacity',
        'EncryptionType': 'Encryption',
        'StreamEnabled': 'Stream Enabled',
        'CreationDateTime': 'Creation Date',
        'ARN': 'ARN',
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
        df.to_excel(writer, index=False, sheet_name='DynamoDB Audit')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['DynamoDB Audit']
        for idx, col in enumerate(df.columns):
            max_length = max((
                df[col].astype(str).map(len).max(),
                len(str(col))
            )) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
    
    logger.info(f"Audit results saved to: {output_path}")
    return output_path

def main() -> int:
    """Main function to run the DynamoDB audit."""
    try:
        # Initialize auditor
        auditor = DynamoDBAuditor()
        
        # Get all tables
        tables = auditor.get_all_tables()
        
        if not tables:
            logger.warning("No DynamoDB tables found or an error occurred")
            return 1
            
        # Save to Excel
        output_file = save_to_excel(tables)
        
        if output_file:
            logger.info(f"DynamoDB audit completed successfully. Results saved to: {output_file}")
            return 0
        else:
            logger.error("Failed to save audit results")
            return 1
            
    except Exception as e:
        logger.error(f"Error running DynamoDB audit: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())

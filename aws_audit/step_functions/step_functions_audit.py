#!/usr/bin/env python3
"""
Step Functions Audit Tool

Collects information about all Step Functions state machines in the account,
including their configuration, execution metrics, and tags.
The results are saved to an Excel spreadsheet for analysis.
"""
import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional, Tuple
import os
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StepFunctionsAuditor:
    def __init__(self):
        """Initialize AWS clients and data structures."""
        self.sfn_client = boto3.client('stepfunctions')
        self.cloudwatch = boto3.client('cloudwatch')
        self.cf_client = boto3.client('cloudformation')
        self.state_machines = []
    
    def get_stack_name_from_arn(self, resource_arn: str) -> str:
        """
        Get the CloudFormation stack name from a resource ARN.
        Returns the stack name as a string or None if not found.
        Uses a more efficient approach that doesn't scan all stacks.
        """
        try:
            # First, try to get stack info from tags (fastest method)
            try:
                response = self.sfn_client.list_tags_for_resource(
                    resourceArn=resource_arn
                )
                tags = {tag['key']: tag['value'] for tag in response.get('tags', [])}
                
                # Check for CloudFormation stack name tag
                stack_name = tags.get('aws:cloudformation:stack-name')
                if stack_name:
                    return stack_name
                    
            except Exception as e:
                logger.debug(f"Error getting tags for {resource_arn}: {str(e)}")
                
            # If tag lookup fails, try direct lookup (slower but more reliable)
            try:
                resource_id = resource_arn.split(':')[-1].split('/')[-1]
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
            
        return None
        
    def get_execution_metrics_30d(self, state_machine_arn: str) -> dict:
        """Get execution metrics for the last 30 days."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=30)
            
            # Get execution metrics
            metrics = {}
            
            # Get execution count
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/States',
                MetricName='ExecutionsStarted',
                Dimensions=[
                    {
                        'Name': 'StateMachineArn',
                        'Value': state_machine_arn
                    },
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour in seconds
                Statistics=['Sum'],
                Unit='Count'
            )
            
            metrics['ExecutionsStarted'] = sum(
                int(point['Sum']) 
                for point in response.get('Datapoints', [])
                if 'Sum' in point
            )
            
            # Get execution time
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/States',
                MetricName='ExecutionTime',
                Dimensions=[
                    {
                        'Name': 'StateMachineArn',
                        'Value': state_machine_arn
                    },
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour in seconds
                Statistics=['Average'],
                Unit='Milliseconds'
            )
            
            if response.get('Datapoints'):
                metrics['AvgExecutionTimeMs'] = sum(
                    point['Average'] 
                    for point in response['Datapoints']
                    if 'Average' in point
                ) / len(response['Datapoints']) if response['Datapoints'] else 0
            else:
                metrics['AvgExecutionTimeMs'] = 0
                
            return metrics
            
        except Exception as e:
            logger.warning(f"Could not get metrics for {state_machine_arn}: {str(e)}")
            return {
                'ExecutionsStarted': -1,
                'AvgExecutionTimeMs': -1
            }
    
    def get_state_machine_tags(self, state_machine_arn: str) -> dict:
        """Get tags for a state machine."""
        try:
            response = self.sfn_client.list_tags_for_resource(
                resourceArn=state_machine_arn
            )
            return {tag['key']: tag['value'] for tag in response.get('tags', [])}
        except Exception as e:
            logger.warning(f"Could not get tags for {state_machine_arn}: {str(e)}")
            return {}
    
    def get_all_state_machines(self) -> List[Dict[str, Any]]:
        """Retrieve all Step Functions state machines in the account."""
        logger.info("Fetching all Step Functions state machines...")
        state_machines = []
        
        try:
            # Get the first page of state machines
            response = self.sfn_client.list_state_machines()
            
            while True:
                for sm in response.get('stateMachines', []):
                    try:
                        # Get state machine details
                        details = self.sfn_client.describe_state_machine(
                            stateMachineArn=sm['stateMachineArn']
                        )
                        
                        # Get execution metrics
                        metrics = self.get_execution_metrics_30d(sm['stateMachineArn'])
                        
                        # Get tags
                        tags = self.get_state_machine_tags(sm['stateMachineArn'])
                        
                        # Get stack information
                        stack_name = self.get_stack_name_from_arn(sm['stateMachineArn'])
                        
                        # Extract environment from name (assuming format: prefix-environment-suffix or prefix-suffix-environment)
                        name_parts = sm['name'].split('-')
                        environment = 'unknown'
                        
                        # Common environment suffixes
                        env_indicators = ['dev', 'prod', 'staging', 'test', 'qa', 'uat', 'preprod']
                        
                        # Check last part for environment
                        if len(name_parts) > 1 and name_parts[-1].lower() in env_indicators:
                            environment = name_parts[-1].lower()
                        # Check second to last part (for names like service-name-dev-1)
                        elif len(name_parts) > 2 and name_parts[-2].lower() in env_indicators:
                            environment = name_parts[-2].lower()
                            
                        # Prepare state machine info
                        state_machine_info = {
                            'Name': sm['name'],
                            'Environment': environment.upper(),
                            'ARN': sm['stateMachineArn'],
                            'Type': details.get('type', 'STANDARD'),
                            'CreationDate': details.get('creationDate', '').strftime('%Y-%m-%d %H:%M:%S') 
                                          if 'creationDate' in details else '',
                            'Status': details.get('status', 'UNKNOWN'),
                            'ExecutionsStarted30d': metrics['ExecutionsStarted'],
                            'AvgExecutionTimeMs': metrics['AvgExecutionTimeMs'],
                            'Tags': ', '.join([f"{k}={v}" for k, v in tags.items()]) if tags else 'None',
                            'StackName': stack_name or 'Not managed by CloudFormation'
                        }
                        
                        state_machines.append(state_machine_info)
                        logger.info(f"Processed state machine: {sm['name']}")
                        
                    except Exception as e:
                        logger.error(f"Error processing state machine {sm.get('name', 'unknown')}: {str(e)}")
                
                # Check if there are more pages
                if 'nextToken' in response:
                    response = self.sfn_client.list_state_machines(
                        nextToken=response['nextToken']
                    )
                else:
                    break
            
            return state_machines
            
        except Exception as e:
            logger.error(f"Error listing state machines: {str(e)}", exc_info=True)
            return []

def save_to_excel(data: List[Dict[str, Any]], filename: str = 'step_functions_audit.xlsx') -> str:
    """Save the audit results to an Excel file.
    
    Args:
        data: List of dictionaries containing state machine data
        filename: Name of the file to save to
        
    Returns:
        str: The absolute path to the saved file
    """
    if not data:
        logger.warning("No data to save")
        return filename
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Define the column order and their display names
    column_mapping = {
        'Name': 'Name',
        'Environment': 'Environment',
        'StackName': 'Stack Name',
        'Type': 'Type',
        'Status': 'Status',
        'ExecutionsStarted30d': 'Executions (30d)',
        'AvgExecutionTimeMs': 'Avg Execution Time (ms)',
        'CreationDate': 'Creation Date',
        'ARN': 'ARN',
        'Tags': 'Tags'
    }
    
    # Create ordered list of columns that exist in the DataFrame
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    
    # Reorder and rename columns
    df = df[existing_columns]
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Save to Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Step Functions Audit')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Step Functions Audit']
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
    """Main function to run the Step Functions audit."""
    try:
        logger.info("Starting Step Functions audit...")
        auditor = StepFunctionsAuditor()
        results = auditor.get_all_state_machines()
        
        if results:
            output_file = 'step_functions_audit.xlsx'
            saved_file = save_to_excel(results, output_file)
            print(f"\nAudit completed successfully! Results saved to: {os.path.abspath(saved_file)}")
            print(f"Total state machines audited: {len(results)}")
        else:
            print("No state machines found or an error occurred during the audit.")
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

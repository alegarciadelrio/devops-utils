#!/usr/bin/env python3
"""
Lambda Audit

Collects information about all Lambda functions,
including their update history, Python version, and invocation metrics.
The results are saved to an Excel spredsheet for analysis.
"""
import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LambdaAuditor:
    def __init__(self):
        """Initialize AWS clients and data structures."""
        self.lambda_client = boto3.client('lambda')
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.functions = []
        
    def get_all_functions(self) -> List[Dict[str, Any]]:
        """Retrieve all Lambda functions in the account."""
        logger.info("Fetching all Lambda functions...")
        functions = []
        marker = None
        
        while True:
            params = {}
            if marker:
                params['Marker'] = marker
                
            try:
                response = self.lambda_client.list_functions(**params)
                functions.extend(response['Functions'])
                
                if 'NextMarker' in response:
                    marker = response['NextMarker']
                else:
                    break
            except Exception as e:
                logger.error(f"Error fetching Lambda functions: {str(e)}")
                break
                
        logger.info(f"Found {len(functions)} Lambda functions")
        return functions
    
    def get_function_update_history(self, function_name: str, max_updates: int = 10) -> List[str]:
        """Get the most recent update timestamps for a Lambda function."""
        try:
            response = self.lambda_client.list_versions_by_function(
                FunctionName=function_name
            )
            
            # Sort versions by LastModified (newest first)
            versions = sorted(
                response.get('Versions', []),
                key=lambda x: x.get('LastModified', ''),
                reverse=True
            )
            
            # Return the last N update timestamps
            return [v['LastModified'] for v in versions[:max_updates]]
            
        except Exception as e:
            logger.warning(f"Could not get update history for {function_name}: {str(e)}")
            return []
    
    def get_invocation_metrics(self, function_name: str) -> Dict[str, int]:
        """Get invocation metrics for different time periods."""
        end_time = datetime.utcnow()
        periods = {
            'last_month': 30,
            'last_3_months': 90,
            'last_year': 365
        }
        
        metrics = {}
        
        for period_name, days in periods.items():
            start_time = end_time - timedelta(days=days)
            
            try:
                response = self.cloudwatch_client.get_metric_statistics(
                    Namespace='AWS/Lambda',
                    MetricName='Invocations',
                    Dimensions=[
                        {
                            'Name': 'FunctionName',
                            'Value': function_name
                        },
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400 * days,  # One data point per period
                    Statistics=['Sum']
                )
                
                # Sum all datapoints for the period
                total = sum(datapoint['Sum'] for datapoint in response.get('Datapoints', []))
                metrics[f'invocations_{period_name}'] = int(total)
                
            except Exception as e:
                logger.warning(f"Could not get {period_name} metrics for {function_name}: {str(e)}")
                metrics[f'invocations_{period_name}'] = 0
        
        return metrics
    
    def get_environment(self, function_name: str, tags: Dict[str, str]) -> str:
        """Determine the environment from Lambda tags or function name."""
        # Check tags first
        env_tags = ['env', 'environment', 'stage', 'deployment']
        for tag in env_tags:
            if tag in tags:
                return tags[tag].lower()
        
        # Check function name patterns
        name_lower = function_name.lower()
        for env in ['dev', 'staging', 'prod', 'production', 'test', 'qa']:
            if f'-{env}' in name_lower or f'_{env}' in name_lower:
                return env
                
        return 'all'
        
    def get_stack_name(self, tags: Dict[str, str]) -> str:
        """Get the CloudFormation stack name from Lambda tags if available."""
        # Check for common CloudFormation stack tags
        stack_tags = ['aws:cloudformation:stack-name', 'cloudformation:stack-name', 'stack-name']
        for tag in stack_tags:
            if tag in tags:
                return tags[tag]
        return ''
    
    def audit_functions(self):
        """Main method to audit all Lambda functions."""
        functions = self.get_all_functions()
        results = []
        
        for func in functions:
            try:
                function_name = func['FunctionName']
                logger.info(f"Processing function: {function_name}")
                
                # Get function tags
                try:
                    tags = self.lambda_client.list_tags(Resource=func['FunctionArn'])['Tags']
                except:
                    tags = {}
                
                # Get update history
                update_dates = self.get_function_update_history(function_name)
                
                # Get invocation metrics
                metrics = self.get_invocation_metrics(function_name)
                
                # Prepare result row
                row = {
                    'FunctionName': function_name,
                    'FunctionArn': func['FunctionArn'],
                    'Runtime': func.get('Runtime', 'N/A'),
                    'LastModified': func.get('LastModified', 'N/A'),
                    'Environment': self.get_environment(function_name, tags),
                    'StackName': self.get_stack_name(tags),
                    'UpdateDates': '\n'.join(update_dates) if update_dates else 'N/A',
                    **metrics
                }
                
                results.append(row)
                
            except Exception as e:
                logger.error(f"Error processing function {func.get('FunctionName', 'unknown')}: {str(e)}")
        
        return results

def save_to_excel(data: List[Dict[str, Any]], filename: str = 'lambda_audit.xlsx') -> None:
    """Save the audit results to an Excel file."""
    if not data:
        logger.warning("No data to save")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Format columns
    column_order = [
        'FunctionName',
        'Environment',
        'StackName',
        'Runtime',
        'LastModified',
        'UpdateDates',
        'invocations_last_month',
        'invocations_last_3_months',
        'invocations_last_year',
        'FunctionArn'
    ]
    
    # Reorder and rename columns
    df = df[column_order]
    df = df.rename(columns={
        'invocations_last_month': 'Invocations (Last 30d)',
        'invocations_last_3_months': 'Invocations (Last 90d)',
        'invocations_last_year': 'Invocations (Last 365d)',
        'Runtime': 'Python Version',
        'LastModified': 'Last Modified',
        'UpdateDates': 'Update History (Last 10)',
        'FunctionArn': 'ARN',
        'FunctionName': 'Function Name',
        'Environment': 'Environment',
        'StackName': 'CloudFormation Stack'
    })
    
    # Save to Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Lambda Audit')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['Lambda Audit']
        for idx, col in enumerate(df.columns):
            max_length = max(
                df[col].astype(str).apply(len).max(),
                len(str(col))
            )
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length + 2, 30)
    
    logger.info(f"Audit results saved to {os.path.abspath(filename)}")

def main():
    """Main function to run the Lambda audit."""
    try:
        auditor = LambdaAuditor()
        results = auditor.audit_functions()
        
        if results:
            output_file = 'lambda_audit.xlsx'
            save_to_excel(results, output_file)
            print(f"\nAudit completed successfully! Results saved to: {os.path.abspath(output_file)}")
            print(f"Total Lambda functions audited: {len(results)}")
        else:
            print("No Lambda functions found or an error occurred during the audit.")
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

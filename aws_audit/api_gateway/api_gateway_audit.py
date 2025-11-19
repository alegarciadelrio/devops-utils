#!/usr/bin/env python3
"""
API Gateway Audit Tool

Collects information about all API Gateway REST APIs and HTTP APIs in the account,
including their configuration, metrics, and tags.
The results are saved to an Excel spreadsheet for analysis.
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

class APIGatewayAuditor:
    def __init__(self):
        """Initialize AWS clients and data structures."""
        self.apigw_client = boto3.client('apigateway')
        self.apigwv2_client = boto3.client('apigatewayv2')
        self.cloudwatch = boto3.client('cloudwatch')
        self.apis = []
        
    def get_api_metrics_30d(self, api_id: str, api_type: str = 'REST') -> dict:
        """Get total requests for the last 30 days across all stages."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=30)
            metrics = {
                'TotalRequests': 0
            }
            
            if api_type == 'REST':
                # For REST APIs
                namespace = 'AWS/ApiGateway'
                
                # First, get all stages for this API
                try:
                    response = self.apigw_client.get_rest_api(restApiId=api_id)
                    api_name = response['name']
                    
                    stages = self.apigw_client.get_stages(restApiId=api_id)
                    stage_names = [stage['stageName'] for stage in stages.get('item', [])]
                except Exception as e:
                    logger.warning(f"Could not get stages for REST API {api_id}: {str(e)}")
                    return metrics
                
                total_requests = 0
                
                for stage in stage_names:
                    dimensions = [
                        {'Name': 'ApiName', 'Value': api_name},
                        {'Name': 'Stage', 'Value': stage}
                    ]
                    
                    # Get request count for this stage
                    try:
                        response = self.cloudwatch.get_metric_statistics(
                            Namespace=namespace,
                            MetricName='Count',
                            Dimensions=dimensions,
                            StartTime=start_time,
                            EndTime=end_time,
                            Period=86400,  # 1 day
                            Statistics=['Sum'],
                            Unit='Count'
                        )
                        
                        if response.get('Datapoints'):
                            stage_requests = sum(
                                int(point.get('Sum', 0))
                                for point in response['Datapoints']
                            )
                            total_requests += stage_requests
                            logger.debug(f"REST API {api_id} stage {stage} requests: {stage_requests}")
                                
                    except Exception as e:
                        logger.warning(f"Error getting metrics for REST API {api_id} stage {stage}: {str(e)}")
                        continue
                
                metrics['TotalRequests'] = total_requests
                logger.info(f"Total requests for REST API {api_id} ({api_name}): {total_requests}")
                
            else:  # HTTP API
                namespace = 'AWS/ApiGateway'
                
                try:
                    # Get API details first to ensure it exists and get its name
                    api = self.apigwv2_client.get_api(ApiId=api_id)
                    
                    # Get all stages for HTTP API
                    stages = self.apigwv2_client.get_stages(ApiId=api_id)
                    stage_names = [stage['StageName'] for stage in stages.get('Items', [])]
                except Exception as e:
                    logger.warning(f"Could not get stages for HTTP API {api_id}: {str(e)}")
                    return metrics
                
                total_requests = 0
                
                for stage in stage_names:
                    dimensions = [
                        {'Name': 'ApiId', 'Value': api_id},
                        {'Name': 'Stage', 'Value': stage}
                    ]
                    
                    try:
                        # Get request count for this stage
                        response = self.cloudwatch.get_metric_statistics(
                            Namespace=namespace,
                            MetricName='Count',
                            Dimensions=dimensions,
                            StartTime=start_time,
                            EndTime=end_time,
                            Period=86400,  # 1 day
                            Statistics=['Sum'],
                            Unit='Count'
                        )
                        
                        if response.get('Datapoints'):
                            stage_requests = sum(
                                int(point.get('Sum', 0))
                                for point in response['Datapoints']
                            )
                            total_requests += stage_requests
                            logger.debug(f"HTTP API {api_id} stage {stage} requests: {stage_requests}")
                                
                    except Exception as e:
                        logger.warning(f"Error getting metrics for HTTP API {api_id} stage {stage}: {str(e)}")
                        continue
                
                metrics['TotalRequests'] = total_requests
                logger.info(f"Total requests for HTTP API {api_id}: {total_requests}")
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Could not get metrics for API {api_id} ({api_type}): {str(e)}")
            return {
                'TotalRequests': -1
            }
    
    def get_rest_apis(self) -> List[Dict[str, Any]]:
        """Retrieve all REST APIs with detailed information."""
        logger.info("Fetching REST APIs...")
        apis = []
        
        try:
            paginator = self.apigw_client.get_paginator('get_rest_apis')
            for page in paginator.paginate():
                for api in page.get('items', []):
                    try:
                        api_id = api['id']
                        api_name = api['name']
                        logger.info(f"Processing REST API: {api_name} (ID: {api_id})")
                        
                        # Get API details
                        try:
                            details = self.apigw_client.get_rest_api(restApiId=api_id)
                            metrics = self.get_api_metrics_30d(api_id, 'REST')
                            
                            # Get resources count
                            resources = self.apigw_client.get_resources(restApiId=api_id)
                            resource_count = len(resources.get('items', []))
                            
                            # Get stages and endpoint configuration
                            stages = self.apigw_client.get_stages(restApiId=api_id)
                            stage_items = stages.get('item', [])
                            stage_names = [stage['stageName'] for stage in stage_items]
                            
                            # Get endpoint URL
                            endpoint_config = details.get('endpointConfiguration', {})
                            endpoint_types = endpoint_config.get('types', [])
                            
                            # For regional endpoints, construct the endpoint URL
                            api_endpoint = ''
                            if stage_items and endpoint_types:
                                region = boto3.Session().region_name
                                api_endpoint = f"https://{api_id}.execute-api.{region}.amazonaws.com"
                                if stage_items:
                                    api_endpoint = f"{api_endpoint}/{stage_items[0]['stageName']}"
                            
                            # Get tags and extract stack name
                            tags_response = self.apigw_client.get_tags(
                                resourceArn=f"arn:aws:apigateway:{boto3.Session().region_name}::/restapis/{api_id}"
                            )
                            tags = tags_response.get('tags', {})
                            stack_name = tags.get('aws:cloudformation:stack-name', '')
                            
                            api_info = {
                                'Name': api_name,
                                'Id': api_id,
                                'Type': 'REST',
                                'Description': details.get('description', ''),
                                'CreatedDate': details.get('createdDate', '').strftime('%Y-%m-%d %H:%M:%S'),
                                'EndpointConfiguration': ','.join(endpoint_types),
                                'ApiKeySource': details.get('apiKeySource', 'NONE'),
                                'ResourceCount': resource_count,
                                'Stages': ','.join(stage_names) if stage_names else 'None',
                                'TotalRequests30d': metrics['TotalRequests'],
                                'ApiEndpoint': api_endpoint,
                                'StackName': stack_name,
                                'Tags': ', '.join([f"{k}={v}" for k, v in tags.items()]) if tags else 'None'
                            }
                            
                            apis.append(api_info)
                            logger.info(f"Successfully processed REST API: {api_name}")
                            
                        except Exception as e:
                            logger.error(f"Error getting details for REST API {api_name} ({api_id}): {str(e)}")
                            # Add basic API info even if details fail
                            apis.append({
                                'Name': api_name,
                                'Id': api_id,
                                'Type': 'REST',
                                'Description': 'Error retrieving details',
                                'CreatedDate': '',
                                'EndpointConfiguration': '',
                                'ApiKeySource': '',
                                'ResourceCount': 0,
                                'Stages': 'Error',
                                'TotalRequests30d': -1,
                                'AvgLatencyMs': -1,
                                'Tags': 'Error retrieving tags'
                            })
                            
                    except Exception as e:
                        logger.error(f"Unexpected error processing REST API: {str(e)}", exc_info=True)
            
            logger.info(f"Successfully fetched {len(apis)} REST APIs")
            return apis
            
        except Exception as e:
            logger.error(f"Fatal error listing REST APIs: {str(e)}", exc_info=True)
            return []
    
    def get_http_apis(self) -> List[Dict[str, Any]]:
        """Retrieve all HTTP APIs with detailed information."""
        logger.info("Fetching HTTP APIs...")
        apis = []
        
        try:
            paginator = self.apigwv2_client.get_paginator('get_apis')
            for page in paginator.paginate():
                for api in page.get('Items', []):
                    try:
                        api_id = api['ApiId']
                        api_name = api['Name']
                        logger.info(f"Processing HTTP API: {api_name} (ID: {api_id})")
                        
                        try:
                            # Get metrics for this API
                            metrics = self.get_api_metrics_30d(api_id, 'HTTP')
                            
                            # Get API details to get the API endpoint
                            api_details = self.apigwv2_client.get_api(ApiId=api_id)
                            
                            # Get integrations
                            integrations = self.apigwv2_client.get_integrations(ApiId=api_id)
                            integration_count = len(integrations.get('Items', []))
                            
                            # Get stages
                            stages = self.apigwv2_client.get_stages(ApiId=api_id)
                            stage_items = stages.get('Items', [])
                            stage_names = [stage['StageName'] for stage in stage_items]
                            
                            # Get API endpoint from API details or construct it
                            api_endpoint = api_details.get('ApiEndpoint', '')
                            if not api_endpoint and stage_items:
                                # If no explicit endpoint, construct it using the first stage
                                region = boto3.Session().region_name
                                api_endpoint = f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage_items[0]['StageName']}"
                            
                            # Get tags and extract stack name
                            tags_response = self.apigwv2_client.get_tags(
                                ResourceArn=f"arn:aws:apigateway:{boto3.Session().region_name}::/apis/{api_id}"
                            )
                            tags = tags_response.get('Tags', {})
                            stack_name = tags.get('aws:cloudformation:stack-name', '')
                            
                            api_info = {
                                'Name': api_name,
                                'Id': api_id,
                                'Type': 'HTTP',
                                'Description': api.get('Description', ''),
                                'CreatedDate': api.get('CreatedDate', '').strftime('%Y-%m-%d %H:%M:%S'),
                                'ProtocolType': api.get('ProtocolType', 'HTTP'),
                                'ApiEndpoint': api_endpoint,
                                'IntegrationCount': integration_count,
                                'Stages': ','.join(stage_names) if stage_names else 'None',
                                'TotalRequests30d': metrics['TotalRequests'],
                                'StackName': stack_name,
                                'Tags': ', '.join([f"{k}={v}" for k, v in tags.items()]) if tags else 'None'
                            }
                            
                            apis.append(api_info)
                            logger.info(f"Successfully processed HTTP API: {api_name}")
                            
                        except Exception as e:
                            logger.error(f"Error getting details for HTTP API {api_name} ({api_id}): {str(e)}")
                            # Add basic API info even if details fail
                            apis.append({
                                'Name': api_name,
                                'Id': api_id,
                                'Type': 'HTTP',
                                'Description': 'Error retrieving details',
                                'CreatedDate': '',
                                'ProtocolType': api.get('ProtocolType', 'HTTP'),
                                'ApiEndpoint': '',
                                'IntegrationCount': 0,
                                'Stages': 'Error',
                                'TotalRequests30d': -1,
                                'AvgLatencyMs': -1,
                                'Tags': 'Error retrieving tags'
                            })
                            
                    except Exception as e:
                        logger.error(f"Unexpected error processing HTTP API: {str(e)}", exc_info=True)
            
            logger.info(f"Successfully fetched {len(apis)} HTTP APIs")
            return apis
            
        except Exception as e:
            logger.error(f"Fatal error listing HTTP APIs: {str(e)}", exc_info=True)
            return []
    
    def get_all_apis(self) -> List[Dict[str, Any]]:
        """Retrieve all APIs (REST and HTTP)."""
        rest_apis = self.get_rest_apis()
        http_apis = self.get_http_apis()
        return rest_apis + http_apis

def save_to_excel(data: List[Dict[str, Any]], filename: str = 'api_gateway_audit.xlsx') -> str:
    """Save the audit results to an Excel file.
    
    Args:
        data: List of dictionaries containing API Gateway data
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
        'Stages': 'Stages',
        'StackName': 'Stack Name',
        'TotalRequests30d': 'Total Requests (30d)',
        'ResourceCount': 'Resource Count',
        'Description': 'Description',
        'CreatedDate': 'Created Date',
        'Type': 'Type',
        'ApiEndpoint': 'API Endpoint',
        'Id': 'ID',
        'Tags': 'Tags'
    }
    
    # Create ordered list of columns that exist in the DataFrame
    existing_columns = [col for col in column_mapping.keys() if col in df.columns]
    
    # Reorder and rename columns
    df = df[existing_columns]
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Save to Excel
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='API Gateway Audit')
        
        # Auto-adjust column widths
        worksheet = writer.sheets['API Gateway Audit']
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
    """Main function to run the API Gateway audit."""
    try:
        logger.info("Starting API Gateway audit...")
        auditor = APIGatewayAuditor()
        results = auditor.get_all_apis()
        
        if results:
            output_file = 'api_gateway_audit.xlsx'
            saved_file = save_to_excel(results, output_file)
            print(f"\nAudit completed successfully! Results saved to: {os.path.abspath(saved_file)}")
            print(f"Total APIs audited: {len(results)}")
        else:
            print("No APIs found or an error occurred during the audit.")
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

#!/bin/bash

APP_NAME=""
DEPLOYMENT_GROUP=""
FUNCTION_NAME=""
REGION="us-east-1"

echo "=== Canary Deployment Monitor ==="
echo ""

# Get latest deployment
echo "📊 Latest Deployment:"
DEPLOYMENT_ID=$(aws deploy list-deployments \
  --application-name $APP_NAME \
  --deployment-group-name $DEPLOYMENT_GROUP \
  --region $REGION \
  --max-items 1 \
  --query 'deployments[0]' \
  --output text)

if [ ! -z "$DEPLOYMENT_ID" ] && [ "$DEPLOYMENT_ID" != "None" ]; then
  echo "Deployment ID: $DEPLOYMENT_ID"
  echo ""
  aws deploy get-deployment \
    --deployment-id $DEPLOYMENT_ID \
    --region $REGION \
    --query 'deploymentInfo.{Status:status,Config:deploymentConfigName,Created:createTime,Overview:deploymentOverview}' \
    --output table
else
  echo "No active deployments"
fi

echo ""
echo "🔀 Traffic Distribution:"
aws lambda get-alias \
  --function-name $FUNCTION_NAME \
  --name live \
  --region $REGION \
  --query '{Version:FunctionVersion,Routing:RoutingConfig}' \
  --output table

echo ""
echo "⚠️  CloudWatch Alarms:"
aws cloudwatch describe-alarms \
  --alarm-name-prefix lambda-canary-demo \
  --region $REGION \
  --query 'MetricAlarms[*].{Name:AlarmName,State:StateValue,Reason:StateReason}' \
  --output table

echo ""
echo "🔄 To continuously monitor, run: watch -n 5 ./monitor-canary.sh"
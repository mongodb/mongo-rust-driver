#!/usr/bin/env bash
set -o errexit  # Exit the script with error if any of the commands fail

# Explanation of required environment variables:
#
# TEST_LAMBDA_DIRECTORY: The root of the project's Lambda sam project.
# DRIVERS_ATLAS_PUBLIC_API_KEY: The public Atlas key for the drivers org.
# DRIVERS_ATLAS_PRIVATE_API_KEY: The private Atlas key for the drivers org.
# DRIVERS_ATLAS_LAMBDA_USER: The user for the lambda cluster.
# DRIVERS_ATLAS_LAMBDA_PASSWORD: The password for the user.
# DRIVERS_ATLAS_GROUP_ID: The id of the individual projects under the drivers org, per language.
# LAMBDA_STACK_NAME: The name of the stack on lambda "dbx-<language>-lambda"
# AWS_REGION: The region for the function - generally us-east-1
#
# Explanation of optional environment variables:
#
# SAM_BUILD_ARGS: Additional arguments to pass to the "sam build" command.

VARLIST=(
TEST_LAMBDA_DIRECTORY
DRIVERS_ATLAS_PUBLIC_API_KEY
DRIVERS_ATLAS_PRIVATE_API_KEY
DRIVERS_ATLAS_LAMBDA_USER
DRIVERS_ATLAS_LAMBDA_PASSWORD
DRIVERS_ATLAS_GROUP_ID
LAMBDA_STACK_NAME
AWS_REGION
)

# Ensure that all variables required to run the test are set, otherwise throw
# an error.
for VARNAME in ${VARLIST[*]}; do
[[ -z "${!VARNAME}" ]] && echo "ERROR: $VARNAME not set" && exit 1;
done

# Set up the common variables
SCRIPT_DIR=${DRIVERS_TOOLS}/.evergreen/aws_lambda
. $SCRIPT_DIR/../handle-paths.sh
. $SCRIPT_DIR/../atlas/setup-variables.sh

# Restarts the cluster's primary node.
restart_cluster_primary ()
{
  echo "Testing Atlas primary restart..."
  curl \
    --digest -u ${DRIVERS_ATLAS_PUBLIC_API_KEY}:${DRIVERS_ATLAS_PRIVATE_API_KEY} \
    -X POST \
    "${ATLAS_BASE_URL}/groups/${DRIVERS_ATLAS_GROUP_ID}/clusters/${FUNCTION_NAME}/restartPrimaries"
}

# Deploys a lambda function to the set stack name.
deploy_lambda_function ()
{
  echo "Deploying Lambda function..."
  sam deploy \
    --debug \
    --stack-name "${FUNCTION_NAME}" \
    --capabilities CAPABILITY_IAM \
    --resolve-s3 \
    --parameter-overrides "MongoDbUri=${MONGODB_URI}" \
    --region ${AWS_REGION}
}

# Get the ARN for the Lambda function we created and export it.
get_lambda_function_arn ()
{
  echo "Getting Lambda function ARN..."
  LAMBDA_FUNCTION_ARN=$(sam list stack-outputs \
    --stack-name ${FUNCTION_NAME} \
    --region ${AWS_REGION} \
    --output json | jq '.[] | select(.OutputKey == "MongoDBFunction") | .OutputValue' | tr -d '"'
  )
  echo "Lambda function ARN: $LAMBDA_FUNCTION_ARN"
  export LAMBDA_FUNCTION_ARN=$LAMBDA_FUNCTION_ARN
}

delete_lambda_function ()
{
  echo "Deleting Lambda Function..."
  sam delete --stack-name ${FUNCTION_NAME} --no-prompts --region us-east-1
}

cleanup ()
{
  delete_lambda_function
}

trap cleanup EXIT SIGHUP

cd "${TEST_LAMBDA_DIRECTORY}"

sam build ${SAM_BUILD_ARGS:-}

deploy_lambda_function

get_lambda_function_arn


check_lambda_output () {
  if grep -q FunctionError output.json
  then
      echo "Exiting due to FunctionError!"
      exit 1
  fi
  cat output.json | jq -r '.LogResult' | base64 --decode
}

aws lambda invoke --function-name ${LAMBDA_FUNCTION_ARN} --log-type Tail lambda-invoke-standard.json > output.json
cat lambda-invoke-standard.json
check_lambda_output

echo "Sleeping 1 minute to build up some streaming protocol heartbeats..."
sleep 60
aws lambda invoke --function-name ${LAMBDA_FUNCTION_ARN} --log-type Tail lambda-invoke-frozen.json > output.json
cat lambda-invoke-frozen.json
check_lambda_output

restart_cluster_primary

echo "Sleeping 1 minute to build up some streaming protocol heartbeats..."
sleep 60
aws lambda invoke --function-name ${LAMBDA_FUNCTION_ARN} --log-type Tail lambda-invoke-outage.json > output.json
cat lambda-invoke-outage.json
check_lambda_output
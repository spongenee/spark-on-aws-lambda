#!/usr/bin/env bash

#Script used to push the image to ECR repository. Please ensure that the role associated with the instance has access to AWS ECR

echo "Starting the PUSH to AWS ECR...."

if [ $# -eq 0 ]
  then
    echo "Please provide the image name"
    echo "Usage: $0 <image-name>"
    exit 1
fi

Dockerimage=$1

# Fetch the AWS account number
aws_account=$(aws sts get-caller-identity --query Account --output text)

if [ $? -ne 0 ]
then
    echo "Failed to get AWS account number. Please check your AWS credentials."
    exit 255
fi

# Get the region defined in the current configuration (default to us-east-1 if none defined)
aws_region=$(aws configure get region)
aws_region=${aws_region:-us-east-1}
reponame="${aws_account}.dkr.ecr.${aws_region}.amazonaws.com/${Dockerimage}:latest"

# Creates a repo if it does not exist
echo "Create or replace if repo does not exist...."
aws ecr describe-repositories --repository-names "${Dockerimage}" > /dev/null 2>&1

if [ $? -ne 0 ]
then
    aws ecr create-repository --repository-name "${Dockerimage}" > /dev/null
fi

# Get the AWS ECR login to pull base image from public ECR
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws 

# Build the docker image and push to ECR
echo "Building the docker image"
docker build -t ${Dockerimage} .

echo "Tagging the Docker image"
docker tag ${Dockerimage} ${reponame}

# Get the AWS ECR login to push the image to private ECR
aws ecr get-login-password --region "${aws_region}" | docker login --username AWS --password-stdin "${aws_account}".dkr.ecr."${aws_region}".amazonaws.com

echo "Pushing the Docker image to AWS ECR"
docker push ${reponame}
#!/bin/bash

# Build a Docker image for our Python application
docker build -t gcr.io/$DEVSHELL_PROJECT_ID/pubsub_pipeline gcp-black-friday-analytics/k8s-twitter-to-pubsub

# Save the image on Google Container Registry
gcloud docker -- push gcr.io/$DEVSHELL_PROJECT_ID/pubsub_pipeline

# Create a Pub/Sub topic that will collect all the tweets
gcloud beta pubsub topics create blackfridaytweets

# Create a Google Container Engine Cluster (enabled to write on Pub/Sub)
gcloud container clusters create gcp-black-friday-analytics-cluster --num-nodes=1 --scopes=https://www.googleapis.com/auth/pubsub

# Acquire the credentials to access the K8S Master
gcloud container clusters get-credentials gcp-black-friday-analytics-cluster

# Deploy our application on the cluster, within a ReplicationController
kubectl create -f gcp-black-friday-analytics/k8s-twitter-to-pubsub/twitter-stream.yaml

# Create the BigQuery dataset
bq mk black_friday_analytics

# Launch the Dataflow Pipeline
cd gcp-black-friday-analytics/dataflow-pubsub-to-bigquery/
mvn compile exec:java -Dexec.mainClass=it.noovle.dataflow.TwitterProcessor -Dexec.args="--streaming --stagingLocation=gs://gcp-black-friday-analytics-staging --project=$DEVSHELL_PROJECT_ID"

# gcp-black-friday-analytics
Analyze Black Friday tweets with a Serverless Data Processing pipeline on Google Cloud Platform.
The architecture includes:
- a Kubernetes cluster to run a Python application that gathers tweets
- a Google Cloud Pub/Sub topic
- a Google Cloud Dataflow pipeline that uses the Natural Language API to get the sentiment of each tweet
- a BigQuery dataset that will contain two tables, respectively for raw and annotated tweets.

## Setup
The repository contains a bash script that automates most of the work. However, there is still something you have to do yourself:

- Create a new Google Cloud Platform project (see https://support.google.com/cloud/answer/6251787?hl=en for instructions).
- Enable the Natural Language API from the Cloud Console (https://console.cloud.google.com/apis/api/language.googleapis.com/overview).
- Open Google Cloud Shell.
- Within Cloud Shell, clone the Git Repository: `git clone https://github.com/LorenzoRidiNoovle/gcp-black-friday-analytics.git gcp-black-friday-analytics`.
- Set the Google Cloud Platform zone: `gcloud config set compute/zone <COMPUTE_ZONE>` (replace `<COMPUTE_ZONE>` with your preferred zone. You can list the available zones with `gcloud compute zones list`).
- Create a GCS bucket as a staging location for Dataflow deployment `gsutil mb -l <LOCATION> gs://gcp-black-friday-analytics-staging` (replace `<LOCATION>` with your preferred location. Available choices are `US`, `EU` or `ASIA`. Choose the location depending on the compute zone you previously selected.
- Replace all occurrences of `<YOUR_PROJECT_ID>` with your actual Project ID within the K8S Yaml file. You can do this with this one-liner, if you are running commands from the Cloud Shell: `sed -i -- 's@<YOUR_PROJECT_ID>@'"$DEVSHELL_PROJECT_ID"'@g' gcp-black-friday-analytics/k8s-twitter-to-pubsub/twitter-stream.yaml`
- [Create a Twitter application](https://apps.twitter.com/app/new) and paste the required information in the gcp-black-friday-analytics/k8s-twitter-to-pubsub/twitter-stream.yaml file.
- launch the start.sh file to provision and start the processing pipeline: `bash start.sh`.

## Credits
The Python app that collects tweets and publish them on Pub/Sub comes from the really nice example "Real-Time Data Analysis with Kubernetes, Cloud Pub/Sub, and BigQuery" published here: https://cloud.google.com/solutions/real-time/kubernetes-pubsub-bigquery.

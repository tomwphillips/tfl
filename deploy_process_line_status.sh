#!/bin/bash

gcloud functions deploy ProcessLineStatus --runtime go111 \
	--region europe-west2 \
    --trigger-resource "tfl-requests" \
	--trigger-event google.storage.object.finalize \
	--set-env-vars LINE_STATUS_PROJECT="$(gcloud config get-value project)",LINE_STATUS_DATASET="tfl",LINE_STATUS_TABLE="predictions",LINE_STATUS_ERROR_BUCKET="tfl-requests-failure",LINE_STATUS_SUCCESS_BUCKET="tfl-requests-success"

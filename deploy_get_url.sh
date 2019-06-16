#!/bin/bash

gcloud functions deploy GetURL --runtime go111 \
    --trigger-topic requester-instruction \
    --region europe-west2

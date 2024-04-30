#!/bin/bash

VM_NAME="vm-abd"

# Iterate through arguments
for arg in "$@"; do
    if [ "$arg" = "-d" ]; then
        has_d_flag=true
        break
    fi
done

if [ "$has_d_flag" = true ]; then
    echo "Only deleting vm instance ..."
    gcloud compute instances stop $VM_NAME \
    && \
    gcloud compute instances delete $VM_NAME --zone=us-central1-a --quiet

else
    echo "Deleting vm instance and saving the machine image ..."
    gcloud compute instances stop $VM_NAME \
    && \
    gcloud compute machine-images delete $VM_NAME --quiet \
    && \
    gcloud compute machine-images create $VM_NAME --source-instance=$VM_NAME --source-instance-zone=us-central1-a --storage-location=us-central1 \
    && \
    gcloud compute instances delete $VM_NAME --zone=us-central1-a --quiet
fi

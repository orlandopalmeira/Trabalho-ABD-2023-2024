#!/bin/bash
# vm-down.sh -d -> Just deletes the vm instance and doesnt save the machine image
# vm-down.sh -> Deletes the VM but also creates the machine image

VM_NAME="vm-abd"
ZONE="us-central1-a"
# ZONE="us-east1-b"

has_d_flag=false
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
    gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet

else
    echo "Deleting vm instance and saving the machine image ..."
    gcloud compute instances stop $VM_NAME \
    && \
    gcloud compute machine-images delete $VM_NAME --quiet \
    && \
    gcloud compute machine-images create $VM_NAME --source-instance=$VM_NAME --source-instance-zone=$ZONE --storage-location=us-central1 \
    && \
    gcloud compute instances delete $VM_NAME --zone=$ZONE --quiet
fi

#!/bin/bash

VM_NAME="vm-abd"
# VM_NAME="vm-abd-orlando"
# ZONE="europe-west1-b"
# ZONE="us-east1-b"
ZONE="us-central1-a"

# gcloud compute ssh vm-abd --project= --zone=[YOUR_ZONE] -- -L 4040:localhost:4040
gcloud compute ssh $VM_NAME --zone=$ZONE -- -L 28080:localhost:28080
# gcloud compute ssh $VM_NAME --zone=$ZONE -- -L 4040:localhost:4040

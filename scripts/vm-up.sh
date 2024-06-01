#!/bin/bash

VM_NAME="vm-abd"
ZONE="us-central1-a"
# ZONE="us-east1-b"

# Check if status contains "RUNNING"
status=$(gcloud compute instances describe $VM_NAME --zone $ZONE 2>/dev/null | grep "status:")
if [[ $status == *"RUNNING"* ]]; then
    echo "Instance is already running. The external IP is:"

else
    echo "Instance is not running."
    echo "Creating vm instance ..."
    gcloud compute instances create $VM_NAME --project=abd-2024-419311 --zone=$ZONE --machine-type=n2-custom-8-16384 --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default --metadata=^,@^ssh-keys=pemicama12:ecdsa-sha2-nistp256\ AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBMD60dM\+a3SyqeaCWZBLbA1hi7NAJjmqoxJQ2253zhM263zET/7xtBX5J4Diff\+4ekffMgy\+wky\+mORZr51pplg=\ google-ssh\ \{\"userName\":\"pemicama12@gmail.com\",\"expireOn\":\"2024-04-04T16:57:46\+0000\"\}$'\n'pemicama12:ssh-rsa\ AAAAB3NzaC1yc2EAAAADAQABAAABAGd48sG0F/5cJuGJjdr1swBenxr6Q4FodqzwvoElpxfRytL1vi4FVdjY\+YjcTegMv3oXXcPvN\+zzqUFP1ZplJjkgsbmivgz8YLVtwSO7BLufZ7RMmAgAX7of/AivzlT4zGlcGAjpMIZvIYRb0gzJqi2k/fvq8p3Klnnd9\+ihjh/BT4N847LgudSZSYRne9irCKiiOtr0rwxP\+Q1EIP6TeKmjthDtv1QI5T1BeDk2K30YB76h4nkTqC5dtASL\+M6iUwP8anCrl5EVDdKKl7NoRGrEjlP4rNOblrzHZ7OXW8jYY9dC0ho78YOZ7Ea37bmUh5KVY620c4cKUfkzVFw0BwU=\ google-ssh\ \{\"userName\":\"pemicama12@gmail.com\",\"expireOn\":\"2024-04-04T16:58:03\+0000\"\}$'\n'pemicama12:ecdsa-sha2-nistp256\ AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBFlklKoY1gp5YsSx4JlRUtEZGSGX4V8mM6\+6Yb4OicVuGNd\+TKtLLYVGTaPwdfO2Y7gsWjxMsvaW5dc5e/b1HG8=\ google-ssh\ \{\"userName\":\"pemicama12@gmail.com\",\"expireOn\":\"2024-04-04T17:00:05\+0000\"\}$'\n'pemicama12:ssh-rsa\ AAAAB3NzaC1yc2EAAAADAQABAAABAQDa9xa34BgYu4ZQ1EyC8if8dMDux1ZDKb\+JL4l405orgXtzIDqd8QO7uVQKs17Fo\+eQAkwFNot4qXvZ2XrmB8mtDuN9\+PYxzeJa6Rk5zXUCs5XEt7Q4HIj7Dlwacp1syw\+nNcBBmh8htcYOB8cyCpVRbX2VXRpLvlLUVRsd2iO75S8GpBESQnu1DJVJHa\+D/IogVdztOiMQrmbfIbfVrutuLCz3dSktp9pYpp\+NflUNM/GHuAI96/L9dt5TcI13i84iPdpv/oQ6ZdR5XAVcu/2EN5QaqR6oZbrcSR6dY/6vZxfX9bQpasjnetm\+vDoQSd0iBafkNtUNO0EvA0tEDyRt\ google-ssh\ \{\"userName\":\"pemicama12@gmail.com\",\"expireOn\":\"2024-04-04T17:00:22\+0000\"\} --no-restart-on-failure --maintenance-policy=TERMINATE --provisioning-model=STANDARD --service-account=276135800230-compute@developer.gserviceaccount.com --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append --min-cpu-platform=Automatic --no-shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring --labels=goog-ec-src=vm_add-gcloud --reservation-affinity=any --source-machine-image=vm-abd
fi

sleep 0.5

# Copies the ssh command to the clipboard and shows it in the terminal
ip=$(gcloud compute instances describe $VM_NAME --zone $ZONE | grep "natIP:" | awk '{print $2}')
echo "ssh -i ~/.ssh/group_gc group@${ip}" | xclip -selection clipboard
echo "' ssh -i ~/.ssh/group_gc group@${ip} ' copiado para o clipboard!"

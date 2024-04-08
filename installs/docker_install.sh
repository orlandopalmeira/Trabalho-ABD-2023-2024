#!/bin/bash

# Update the package index
sudo apt update

# Install necessary packages to allow apt to use a repository over HTTPS
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

# Add Docker's official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Set up the stable Docker repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update the package index again
sudo apt update

# Install Docker Engine
sudo apt install -y docker-ce docker-ce-cli containerd.io

# Verify Docker installation
sudo docker run hello-world

# Manage Docker as a non-root user (optional)
sudo usermod -aG docker $USER

# Activate changes to group
newgrp docker

echo "Docker has been installed successfully."

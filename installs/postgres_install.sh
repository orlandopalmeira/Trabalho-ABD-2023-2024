#!/bin/bash

# Update the package index
sudo apt update

# Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# Check PostgreSQL status
sudo systemctl start postgresql

# Access PostgreSQL prompt as postgres user
#sudo -u postgres psql

# Set password for the postgres user (optional)
# sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'new_password';"

# Create a new PostgreSQL user (optional)
# sudo -u postgres psql -c "CREATE USER myuser WITH PASSWORD 'mypassword';"

# Create a new PostgreSQL database (optional)
# sudo -u postgres psql -c "CREATE DATABASE mydatabase;"

echo "PostgreSQL has been installed successfully."

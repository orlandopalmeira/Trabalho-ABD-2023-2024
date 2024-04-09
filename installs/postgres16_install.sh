sudo sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql-16

# Access PostgreSQL prompt as postgres user
#sudo -u postgres psql

# Set password for the postgres user (optional)
# sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'new_password';"

# Create a new PostgreSQL user (optional)
# sudo -u postgres psql -c "CREATE USER myuser WITH PASSWORD 'mypassword';"

# Create a new PostgreSQL database (optional)
# sudo -u postgres psql -c "CREATE DATABASE mydatabase;"

echo "PostgreSQL has been installed successfully."

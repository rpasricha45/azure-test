#!/bin/bash

# Navigate to the application directory
cd /home/site/wwwroot

# Clean up any existing packages
echo "Cleaning up existing packages..."
pip freeze | xargs pip uninstall -y

# Upgrade pip and install requirements
echo "Upgrading pip..."
python -m pip install --upgrade pip
echo "Installing requirements..."
pip install -r requirements.txt

# Create necessary directories
echo "Creating directories..."
mkdir -p data/test
mkdir -p output

# Start the Flask application with gunicorn
echo "Starting application..."
gunicorn --bind=0.0.0.0:8000 --timeout 600 application:app
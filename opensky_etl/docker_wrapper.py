#!/usr/bin/env python
"""
Docker wrapper script that handles imports properly and loads environment variables.
"""
import os
import sys
import importlib.util
import subprocess
from dotenv import load_dotenv

def run_main():
    """Run the main.py file with proper path setup."""
    # Add current directory to path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, current_dir)
    
    # Print current directory contents
    print(f"Files in current directory: {os.listdir(current_dir)}")
    
    # Check if .env file exists and load it
    env_file = os.path.join(current_dir, '.env')
    if os.path.exists(env_file):
        print(f".env file found at {env_file}")
        load_dotenv(env_file)
        # Print some environment variables (safely, without the actual values)
        env_vars = ['OPENSKY_USERNAME', 'OPENSKY_PASSWORD', 'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_NAME']
        for var in env_vars:
            value = os.environ.get(var)
            print(f"Environment variable {var}: {'SET' if value else 'NOT SET'}")
    else:
        print(f"Warning: .env file not found at {env_file}")
        
        # Check other possible locations
        alt_env = os.path.join(current_dir, '.env.template')
        if os.path.exists(alt_env):
            print(f"Found alternative .env file at {alt_env}")
            load_dotenv(alt_env)
    
    # Find all py files in the current directory
    py_files = [f for f in os.listdir(current_dir) if f.endswith('.py')]
    print(f"Python files in root: {py_files}")
    
    # Find all directories that might be packages
    dirs = [d for d in os.listdir(current_dir) 
            if os.path.isdir(os.path.join(current_dir, d)) and 
            not d.startswith('.') and
            not d == '__pycache__']
    print(f"Potential package directories: {dirs}")
    
    # Print current PYTHONPATH
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH', '')}")
    print(f"sys.path: {sys.path}")
    
    # Run the main.py
    main_path = os.path.join(current_dir, 'main.py')
    if os.path.exists(main_path):
        print(f"Running {main_path}...")
        # Use subprocess to run with the current environment
        subprocess.run([sys.executable, main_path], check=True)
    else:
        print(f"Error: {main_path} not found!")
        sys.exit(1)

if __name__ == "__main__":
    run_main()
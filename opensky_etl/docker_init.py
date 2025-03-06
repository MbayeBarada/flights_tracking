"""
Docker initialization file to set up the Python path correctly.
Place this at the root of your project and import it in your main.py.
"""
import os
import sys

# Add the current directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Add opensky_etl to the path if it exists (using correct lowercase)
opensky_path = os.path.join(os.path.dirname(__file__), 'opensky_etl')
if os.path.exists(opensky_path):
    sys.path.insert(0, opensky_path)

# Print system path for debugging
print(f"Python path: {sys.path}")
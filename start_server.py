#! /usr/bin/env python
import sys
import os

PROJECT_ROOT = os.path.dirname(__file__)

sys.path.append(os.path.join(PROJECT_ROOT, "src"))
sys.path.append(os.path.join(PROJECT_ROOT, "src", "server"))

from src.server import start_server
start_server()

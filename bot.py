# -*- coding: utf-8 -*-
import sys
import os

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_DIR)

from app.main import main

if __name__ == "__main__":
    main()
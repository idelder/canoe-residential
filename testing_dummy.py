"""
For testing code snippets
"""

import pandas as pd
from setup import config
import requests
import utils
import os
import urllib.request
import zipfile
import time
import sqlite3
import numpy as np
from matplotlib import pyplot as pp
from datetime import datetime

print(config.aeo_techs.reset_index().set_index(['end_uses','fuel']))
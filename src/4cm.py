
import os
import sys
import logging
from flask import Flask

import basc_py4chan

app = Flask(__name__)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@app.route('/')
def getslash():
    return 'app deployed'

app.run(host='0.0.0.0', port=8080, debug=True)

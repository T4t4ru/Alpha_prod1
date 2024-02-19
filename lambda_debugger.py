import json
import logging
import os
from base64 import b64decode

logger = logging.getLogger()
logger.setLevel(logging.INFO)
log = logger.info


def lambda_handler(event, context=None):
    log(f'raw_input: \n{event}')

    j = json.loads(b64decode(event["body"]).decode())
    log(f"Decoded payload: {j}")

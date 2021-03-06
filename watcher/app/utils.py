import logging
import hashlib
import json
import sys
import os
from dateutil.parser import parse
from decimal import Decimal
from numbers import Number


def get_log_level():
    level = os.environ.get('LOG_LEVEL')
    if level:
        return int(level)
    return logging.DEBUG

LOG_FILENAME = 'application.log'
logging.basicConfig(filename=LOG_FILENAME, level=get_log_level())
log = logging.getLogger(name='Quick')
log.addHandler(logging.StreamHandler())


def is_date(string):
    try:
        parse(string)
        return True
    except ValueError:
        return False


def is_numeric(value):
    try:
        return isinstance(Decimal(value), Number)
    except:
        return False

def hash_file(file_path, BUF_SIZE = 65536):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def ext(file_path):
    return file_path.split('.')[-1:][0].lower()


def pp(item, die=0, label=''):
    if label:
        print(label)
    print(json.dumps(item, indent=2, default=lambda it: str(it)))
    if die:
        sys.exit()

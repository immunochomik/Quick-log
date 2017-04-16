import logging
import hashlib
import json
import sys


LOG_FILENAME = 'log/application.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
log = logging.getLogger(name='Quick')
log.addHandler(logging.StreamHandler())


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

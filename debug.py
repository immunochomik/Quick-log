import json
import sys


def pp(item, die=0, label=''):
    if label:
        print(label)
    print(json.dumps(item, indent=2, default=lambda it: str(it)))
    if die:
        sys.exit()

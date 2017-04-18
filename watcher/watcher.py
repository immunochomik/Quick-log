#!/usr/bin/python3

import sys
import time
import os
import requests

from app import Processor
from app.waiter import Waiter, WaiterException
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from app.utils import log

DEFAULT_PATH = '/inserts/'
ES_HOST = 'http://localhost'
ES_PORT = '9200'


class ESWaiter(Waiter):
    def _check(self):
        known_version = False
        try:
            res = requests.get('{0}:{1}'.format(ES_HOST, ES_PORT))
            status = res.status_code
            known_version = res.text.find('version') != -1
        except Exception as e:
            status = e.__class__
        self._log('Es waiter checks if Elasticsearch is reachable %s' % status)
        return status == 200 and known_version


class WatcherHandler(FileSystemEventHandler):
    def __init__(self, processor):
        self.processor = processor
        super(WatcherHandler, self).__init__()

    def _process(self, src_path):
        if os.path.isfile(src_path) and self.processor.need_process(src_path):
            self.processor.process(src_path)

    def on_modified(self, event):
        log.info('On modified: %s' % event)
        self._process(event.src_path)

    def on_moved(self, event):
        log.info('On moved: %s' % event)
        self._process(event.src_path)

    def on_created(self, event):
        log.info('On created: %s' % event)
        self._process(event.src_path)

    def on_any_event(self, event):
        log.debug('On any event: %s' % event)


def watch_directory(processor):
    event_handler = WatcherHandler(processor)
    observer = Observer()
    observer.schedule(event_handler, processor.dir_path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    # wait for elastic search to start
    waiter = ESWaiter(logger=log, max_duration=2, retries=2)
    try:
        waiter.wait()
    except WaiterException:
        log.exception('Exception waiting for Elasticsearch to start')
        sys.exit(1)
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    log.info('Path is ' + path)
    processor = Processor(dir_path=path)
    processor.start_processing()
    watch_directory(processor)


import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app import log, Processor

DEFAULT_PATH = '/insert/'


class WatcherHandler(FileSystemEventHandler):
    def __init__(self, processor):
        self.processor = processor
        super(WatcherHandler, self).__init__()

    def on_modified(self, event):
        log.info('On modified: %s' % event)
        if self.processor.need_process(event.src_path):
            self.processor.process(event.src_path)

    def on_created(self, event):
        log.info('On created: %s' % event)
        if self.processor.need_process(event.src_path):
            self.processor.process(event.src_path)

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
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    log.info('Path is ' + path)
    processor = Processor(dir_path=path)
    processor.start_processing()
    watch_directory(processor)


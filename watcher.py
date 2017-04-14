import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from debug import pp

DEFAULT_PATH = '/insert/'

class MyHandler(FileSystemEventHandler):
    def on_modified(self, event):
        pp(event, label='On modified:')

    def on_created(self, event):
        pp(event, label='On created:')

    def on_any_event(self, event):
        pp(event, label='On any event:')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    print('Path is ' + path)
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

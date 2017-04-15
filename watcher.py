import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app.debug import pp
from app import log, Manager, Processor

DEFAULT_PATH = '/insert/'


class WatcherHandler(FileSystemEventHandler):
    def on_modified(self, event):
        pp(event, label='On modified:')

    def on_created(self, event):
        pp(event, label='On created:')

    def on_any_event(self, event):
        pp(event, label='On any event:')


def watch_directory(path):
    event_handler = WatcherHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def start_processing(manager, processor):
    todo = manager.get_files_to_proces()
    for file in todo:
        processor.generate_dashboard(file)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    log.info('Path is ' + path)
    manager = Manager(dir_path=path)
    processor = Processor()
    start_processing(manager=manager, processor=processor)
    watch_directory(manager.dir_path)


import os
from collections import defaultdict
from .utils import hash_file, ext, log
from .indexers import Indexer


class Processor(object):
    known_ext = ['json', 'csv']

    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.done = self._get_done()
        log.debug('Processor crated')

    def start_processing(self):
        todo = self.get_files_to_process()
        for file in todo:
            self.process(file)
            self.generate_dashboard(file)

    def get_files_to_process(self):
        log.debug('get files to process')
        for file in os.listdir(self.dir_path):
            if self.need_process(file):
                yield file

    def process(self, file_path):
        if file_path.find(self.dir_path) == -1:
            file_path = os.path.join(self.dir_path, file_path)
        log.info('Process ' + file_path)
        try:
            Indexer.make(file_path).index_file()
            self.mark_done(file_path)
            return True
        except:
            log.exception('Error in processing of ' + file_path)
            return False

    def generate_dashboard(self, file_path):
        pass

    def need_process(self, file_path):
        if not ext(file_path) in self.known_ext:
            return False
        return not self.was_processed(file_path)

    def was_processed(self, file_path):
        was_done = self.done.get(file_path)
        return was_done and was_done.get(hash_file(file_path))

    def mark_done(self, file_path):
        self.done[file_path][hash_file(file_path)] = True

    def close(self):
        # persist done
        pass

    @staticmethod
    def _get_done():
        # retrieve done from persisted
        d = {}
        return defaultdict(dict, d)

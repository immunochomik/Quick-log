import os
import logging
from elasticsearch import Elasticsearch
from file_utils import hash_file
from collections import defaultdict

LOG_FILENAME = 'log/application.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
log = logging.getLogger(name='Quick')
log.addHandler(logging.StreamHandler())

es = Elasticsearch(hosts={"host": "localhost", "port": 9200})


def ext(file_path):
    return file_path.split('.')[-1:].lower()


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
        log.info('Process ' + file_path)
        try:
            Indexer.make(file_path).index()
            self.mark_done(file_path)
        except:
            log.exception('Error in processing of ' + file_path)

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


class Indexer(object):
    @staticmethod
    def make(file_path):
        extension = ext(file_path)
        if extension == 'json':
            return JSONIndexer(file_path)
        if extension == 'csv':
            return CSVIndexer(file_path)

    def __init__(self, file_path):
        self.file_path = file_path

    def make_index(self):
        pass

    def index(self):
        pass

    def _mapping(self):
        pass

class CSVIndexer(Indexer):
    pass

class JSONIndexer(Indexer):
    pass
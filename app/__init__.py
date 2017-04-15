import logging
from elasticsearch import Elasticsearch
import glob

LOG_FILENAME = 'log/application.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
log = logging.getLogger(name='Quick')
log.addHandler(logging.StreamHandler())

es = Elasticsearch(hosts={"host": "localhost", "port": 9200})

class Processor(object):
    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.cache = {}
        log.debug('Processor crated')

    def get_files_to_proces(self):
        log.debug('get files to process')

        return []

    def was_processed(self, file_path):
        log.debug('was processed ?')
        return False

    def process(self, file_path):
        log.info('Process ' + file_path)
        pass

    def _mapping(self):
        return {}

    def generate_dashboard(self, file_path):
        pass

    def start_processing(self):
        todo = self.get_files_to_proces()
        for file in todo:
            self.process(file)
            self.generate_dashboard(file)


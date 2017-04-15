import logging
from elasticsearch import Elasticsearch

LOG_FILENAME = 'log/application.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)
log = logging.getLogger(name='Quick')

es = Elasticsearch(hosts={"host": "localhost", "port": 9200})

class Manager(object):
    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.cache = {}
        log.debug('Manager crated')

    def get_files_to_proces(self):
        log.debug('get files to process')
        return []

    def was_processed(self, file_path):
        log.debug('was processed ?')
        return False

class Processor(object):

    def process(self, file_path):
        log.info('Process ' + file_path)
        pass

    def _mapping(self):
        return {}

    def generate_dashboard(self):
        pass



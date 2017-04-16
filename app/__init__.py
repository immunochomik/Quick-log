import os
import logging
import json
import csv
from elasticsearch import Elasticsearch
from .file_utils import hash_file
from collections import defaultdict
from os import path as pa

LOG_FILENAME = 'log/application.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
log = logging.getLogger(name='Quick')
log.addHandler(logging.StreamHandler())

es = Elasticsearch(hosts={"host": "localhost", "port": 9200})


def ext(file_path):
    return file_path.split('.')[-1:][0].lower()


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


class Indexer(object):
    extension_setting = 'iconf'
    indexing_settings = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        },
        'mapping': {

        }
    }
    recreate_index = False
    default_type = 'log'
    max_bulk = 1000

    @staticmethod
    def make(file_path):
        extension = ext(file_path)
        if extension == 'json':
            return JSONIndexer(file_path)
        if extension == 'csv':
            return CSVIndexer(file_path)

    def __init__(self, file_path):
        self.file_path = file_path
        self.dir = pa.dirname(file_path)
        self.basename = pa.basename(file_path)
        self.trunk = self.basename.split('.')[:1][0]
        self.type = self.default_type
        self.user_settings = self.load_settings()

    def load_settings(self):
        settings_file = pa.join(self.dir, self.trunk + '.' + self.extension_setting)
        if pa.exists(settings_file):
            with open(settings_file) as sf:
                return json.loads(sf.read())

    def make_index(self):
        request_body = dict(self.indexing_settings)
        request_body['mapping'] = self._mapping()

        log.info("creating '%s' index..." % self.trunk)
        res = es.indices.create(index=self.trunk, body=request_body)
        log.info(" response: '%s'" % res)


    def index_file(self):
        if not es.indices.exists(self.trunk):
            self.make_index()
        elif self.recreate_index:
            log.info("deleting '%s' index..." % self.trunk)
            res = es.indices.delete(index=self.trunk)
            log.info(" response: '%s'" % res)
            self.make_index()
        self._index_content()
        pass

    def _mapping(self):
        return self.user_settings.get('mapping') or self._generate_mapping()

    def make_id(self, counter, data_dict):
        id_fields = self.user_settings.get('id_fields')
        if id_fields:
            return '_'.join([data_dict[key] for key in id_fields])
        return counter

    def _index_content(self):
        c = 0
        bulk_data = []
        for item in self._documents_generator():
            _id = self.make_id(c, item)
            op_dict = {
                'index': {
                    '_index': self.trunk,
                    '_type': self.type,
                    '_id': _id
                }
            }
            bulk_data.append(op_dict)
            bulk_data.append(item)
            c += 1
            if len(bulk_data) > self.max_bulk * 2:
                res = es.bulk(index=self.type, body=bulk_data, refresh=True)
                log.debug(res)
                bulk_data = []

    def _generate_mapping(self):
        raise NotImplemented('Method not implemented')

    def _documents_generator(self):
        raise NotImplemented('Method not implemented')


class CSVIndexer(Indexer):
    delimiter = ','
    quotechar = '"'

    def _generate_mapping(self):
        pass

    def _documents_generator(self):
        with open(self.file_path, 'rb') as fd:
            reader = csv.reader(fd, delimiter=self.delimiter, quotechar=self.quotechar)
            header = [item.lower() for item in reader.next()]
            for row in reader:
                data_dict = {}
                for i in range(len(row)):
                    data_dict[header[i]] = row[i]
                yield data_dict


class JSONIndexer(Indexer):
    pass
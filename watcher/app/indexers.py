import json
import csv
import os
from os import path as pa
from elasticsearch import Elasticsearch

from .utils import ext, log, is_date, is_numeric


class Indexer(object):
    extension_setting = 'meta'
    indexing_settings = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }
    }
    recreate_index = os.environ.get('QL_INDEXER_RECREATE', False)
    default_type = os.environ.get('QL_INDEXER_DEF_TYPE', 'log')
    max_bulk = 1000
    keyword_suffix = '_key'

    @staticmethod
    def make(file_path):
        extension = ext(file_path)
        if extension == 'json':
            return JSONIndexer(file_path)
        if extension == 'csv':
            return CSVIndexer(file_path)

    def __init__(self, file_path, es=None):
        self.file_path = file_path
        self.dir = pa.dirname(file_path)
        self.basename = pa.basename(file_path)
        self.trunk = self.basename.split('.')[:1][0]
        self.type = self.default_type
        self.user_settings = self.load_settings()
        self.index_name = self.trunk.lower()
        self.transform = self._get_transform()
        self.es = es or Elasticsearch(hosts=[{"host": "localhost", "port": 9200}])

    def load_settings(self):
        settings_file = pa.join(self.dir, self.trunk + '.' + self.extension_setting)
        if pa.exists(settings_file):
            with open(settings_file) as sf:
                return json.loads(sf.read())
        return {}

    def make_index(self):
        request_body = dict(self.indexing_settings)
        request_body['mappings'] = self._mapping()

        log.info("creating '%s' index..." % self.index_name)
        res = self.es.indices.create(index=self.index_name, body=request_body)
        log.info(" response: '%s'" % res)

    def index_file(self):
        if not self.es.indices.exists(self.index_name):
            self.make_index()
        elif self.recreate_index:
            log.info("deleting '%s' index..." % self.index_name)
            res = self.es.indices.delete(index=self.index_name)
            log.info(" response: '%s'" % res)
            self.make_index()
        self._index_content()
        pass

    def _mapping(self):
        return self.user_settings.get('mappings') or self._generate_mapping()

    def make_id(self, counter, data_dict):
        id_fields = self.user_settings.get('id_fields')
        if id_fields:
            return '_'.join([data_dict[key] for key in id_fields])
        return counter

    def _index_content(self):
        c = 0
        batch = []
        for item in self._documents_generator():
            op_dict = {
                'index': {
                    '_index': self.index_name,
                    '_type': self.type,
                    '_id': self.make_id(c, item)
                }
            }
            batch.append(op_dict)
            batch.append(item)
            c += 1
            if len(batch) > self.max_bulk * 2:
                batch = self.insert_batch(batch)
        if batch:
            self.insert_batch(batch)

    def _generate_mapping(self):
        props = {}
        for key, value in self._first_document().items():
            if is_numeric(value):
                props[key] = {
                    'type': 'double',
                }
                continue
            if is_date(value):
                props[key] = {
                    'type': 'date',
                    'format': 'strict_date_optional_time||epoch_millis'
                }
                continue
            props[key] = {
                'type': 'text'
            }
        return {
            self.type: {
                'dynamic_templates': [
                    {
                        'keywords': {
                            'match_mapping_type': 'string',
                            'match': '*' + self.keyword_suffix,
                            'mapping': {
                                'type': 'keyword'
                            }
                        }
                    }
                ],
                'dynamic': 'true',
                'properties': props
            }
        }

    def _first_document(self):
        for item in self._documents_generator(suffix_keyword=False):
            return item

    def _documents_generator(self, **kwargs):
        for each in self._concrete_doc_generator(**kwargs):
            yield self.transform(each) if self.transform else each

    def insert_batch(self, bulk_data):
        log.debug('Insert batch of size %s' % str(len(bulk_data) / 2))
        res = self.es.bulk(index=self.type, body=bulk_data, refresh=True)
        log.info("Inserted {}".format(len(res['items'])))
        log_max_errors = 5
        if res['errors']:
            for item in res['items']:
                if item['index']['error']:
                    log.error(item)
                    log_max_errors -= 1
                    if log_max_errors < 0:
                        log.warn('Possibly there was more errors in that batch, but we log only first few.')
                        break
        log.debug(res)
        return []

    def _get_transform(self):
        python_file = pa.join(self.dir, self.trunk + '.py')
        if pa.exists(python_file):
            import importlib.util
            spec = importlib.util.spec_from_file_location("fake.module", python_file)
            foo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(foo)
            return foo.transform

    def _concrete_doc_generator(self, **kwargs):
        raise NotImplemented('Method not implemented')


class CSVIndexer(Indexer):

    def _concrete_doc_generator(self, suffix_keyword=True):
        with open(self.file_path, 'r') as fd:
            reader = self.__make_reader(fd)
            header = [item.lower() for item in next(reader)]
            for row in reader:
                data_dict = {}
                for i in range(len(row)):
                    data_dict[header[i]] = row[i]
                    if suffix_keyword and not is_numeric(row[i]) and not is_date(row[i]):
                        data_dict[header[i] + self.keyword_suffix] = row[i]
                yield data_dict

    @staticmethod
    def __make_reader(fd):
        return csv.reader(fd, delimiter=',', quotechar='"')


class JSONIndexer(Indexer):
    pass
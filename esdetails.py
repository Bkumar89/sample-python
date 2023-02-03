from elasticsearch import Elasticsearch, helpers
from elasticsearch.connection import create_ssl_context
import ssl
import json
import logging
import urllib3
import datetime
import time
from lib.logger import Logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ElasticDB(object):
    def __init__(self, elastic_setting=None):
        self.logger = Logger().get_logger(logger_name="elasticdb-" + str(datetime.datetime.now()),
                                               log_level=logging.INFO)

        # no cafile!
        ssl_context = create_ssl_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        self.es_setting = elastic_setting
        self.es = Elasticsearch(
            self.es_setting["url"].split(','),
            http_auth=(self.es_setting["user"], self.es_setting["password"]),
            ssl_context=ssl_context,
            verify_certs=False

        )
    def bulk_insert(self, data=None, retry_count=3):
        for retry_count_iter in range(retry_count):
            try:
                if retry_count_iter > 0:
                    self.logger.info("retry check: " + str(retry_count_iter))
                    time.sleep(.5)
                result = self.es.bulk(body=data)
                if result.get("errors"):
                    logging.error("Error occured when data was stored in ES")
                    logging.error(json.dumps(result))
                self.logger.debug(result)
                return
            except Exception as ex:
                self.logger.error("Error - %s", str(ex))

    def insert_or_update(self, index=None, type=None, id_field=None, remove_id_field=False, add_datetime=False,
                         data_list=None):
        bulk_upload_size = 1048576 #1MB
        ts = datetime.datetime.utcnow().isoformat()
        try:
            if data_list is not None:
                bulk_data = ''
                for data in data_list:
                    bulk_action_update = {
                        "update": {"_id": None, "_type": None, "_index": None, "retry_on_conflict": 3}
                    }
                    bulk_update_body = {"doc": None, "doc_as_upsert": True}
                    if remove_id_field:
                        bulk_action_update['update']['_id'] = data.pop(id_field)
                    else:
                        bulk_action_update['update']['_id'] = data.get(id_field)

                    if add_datetime:
                        data["@timestamp"] = ts

                    bulk_action_update['update']['_type'] = type
                    bulk_action_update['update']['_index'] = index
                    bulk_update_body['doc'] = data

                    bulk_data += json.dumps(bulk_action_update, default=str) + '\n' + json.dumps(bulk_update_body, default=str) + '\n'

                    if len(bulk_data) >= bulk_upload_size:
                        self.bulk_insert(bulk_data)
                        self.logger.debug(bulk_data)
                        self.logger.debug("Debug payload length: %s bytes", len(bulk_data))
                        bulk_data = ""

                    if len(bulk_data) > 0:
                        self.bulk_insert(bulk_data)
                        self.logger.debug(bulk_data)
                        self.logger.debug("Debug payload length: %s bytes", len(bulk_data))

        except Exception as ex:
            self.logger.error("Error - %s", str(ex))

    def insert(self, index=None, type=None, id_field=None, data_list=None, add_datetime=False):
        bulk_upload_size = 1048576
        ts = datetime.datetime.utcnow().isoformat()
        try:
            if data_list is not None:
                bulk_data = ''
                for data in data_list:
                    bulk_action_insert = {"index": {}}
                    bulk_insert_body = {}

                    if index:
                        bulk_action_insert['index']['_index'] = index
                    if type:
                        bulk_action_insert['type']['_type'] = type
                    if id_field:
                        bulk_action_insert['index']['_id'] = data.get(id_field)

                    bulk_insert_body.update(data)

                    if add_datetime:
                        bulk_insert_body['@timestamp'] = ts

                    bulk_data += json.dumps(bulk_action_insert, default=str) + '\n' + json.dumps(bulk_insert_body, default=str) + '\n'

                    if len(bulk_data) >= bulk_upload_size:
                        self.bulk_insert(bulk_data)
                        self.logger.debug(bulk_data)
                        self.logger.debug("Bulk payload: %s bytes", len(bulk_data))
                        bulk_data = ""

                if len(bulk_data) > 0
                    self.bulk_insert(bulk_data)
                    self.logger.debug(bulk_data)
                    self.logger.debug("Bulk payload: %s bytes", len(bulk_data))
                    bulk_data = ""

        except Exception as ex:
            self.logger.error("Error - %s", str(ex))

    def delete_index(self, index=None):
        try:
            self.es.indices.delete(index=index, ignore=[400, 404])
        except Exception as ex:
            self.logger.error("Error - %s", str(ex))
            self.logger.error("Execution error", exc_info=True)

    def deleye_by_query(self, index=None, body=None, params=None, headers=None):
        try:
            self.es.delete_by_query(index=index, body=body, params=params, headers=headers)
        except Exception as error:
            self.logger.error("error - as %s", str(error))

    def get_all_documents_by_query(self, index=None, type=None, query=None):
        result = []
        try:
            result = helpers.scan(client=self.es, index=index, doc_type=type, query=query)
        except Exception as error:
            self.logger.error("error - as %s", str(error))
        finally:
            return result

    def get_document_by_query(self, index=None, type=None, body=None, sort=None, size=None, filter_path=None):
        result = {}
        try:
            result = self.es.search(
                index=index, doc_type=type, body=body, sort=sort, size=size, filter_path=filter_path
            )
        except Exception as error:
            self.logger.error(error)
        finally:
            return result

    def create_index(self, index_name, index_mapping):
        try:
            self.es.indices.create(index=index_name, body=index_mapping)
        except Exception as error:
            self.logger.error("Error as - $s", str(error))




from elasticsearch import Elasticsearch, ElasticsearchException
import json
from configuration import mConfiguration

class theDatabaseHandler:
    """A class to managa communication with an ElasticSearch database"""
    def __init__(self, config):
        self.config = config
        self.elasticsearch = Elasticsearch(config.ES['connection_initialiser'])

    def push_data(self, index, doc_type, body, ID=None):
        """Push any kind of data to the ElasticSearch DB, only for unit testing"""
        if not ID:
            return self.elasticsearch.index(index=index, doc_type=doc_type, body=body)['result']
        else:
            return self.elasticsearch.index(index=index, doc_type=doc_type, body=body, id=ID)['result']


    def init_mapping(self):
        """This member function initializes the database with apropriate mapping, if the underlying class structure is changed so should this function"""
        mapping = {}
        mapping["mappings"] = {}
        mapping["mappings"]["doc"] = {}
        mapping["mappings"]["doc"]["properties"] = {}
        mapping["mappings"]["doc"]["properties"]["full_text"] = {}
        mapping["mappings"]["doc"]["properties"]["full_text"]["type"] = "text"
        mapping["mappings"]["doc"]["properties"]["publish_date"] = {}
        mapping["mappings"]["doc"]["properties"]["publish_date"]["type"] = "date"
        mapping["mappings"]["doc"]["properties"]["publish_date"]["format"] = "yyyy-MM-dd HH:mm:ss"
        mapping["mappings"]["doc"]["properties"]["publish_date"]["ignore_malformed"] = "true"
        try:
            self.elasticsearch.indices.create(
                index=self.config.ES['index'], body=mapping)
        except ElasticsearchException as es1:
            print(es1)

    def loadFromJsonToES(self, jsonPath, index):
        with open(jsonPath , encoding="utf8") as jsonfile:
             data = json.load(jsonfile)
        for jsonLine in data:
            #debug
            print("inserting : \n", jsonLine)

            self.push_data(index, self.config.ES['doc_type'], jsonLine)

    def iterate_whole_es(self, mIndex, chunk_size, process_data_function, emptyResult='', maxLineCount=-1, dummy_argument = None):
        """Function to iterate through the whole ES database, and processing the data with the
        @param process_data_function: the function that will be called to process a chunk of responses, it will recieve previous results in second argument (None in first iteration)
        @param chunk_size: Prefereably the number of entries in a single response (not guarantied)
        @param mIndex: str, the name of the ES index that is to be scrolled
        @maxLineCount  , if = -1 iterate all the documents, if not, iterate over maxLineCount*chunk_size hits // for debug purposes
        """
        body = {}
        result = None
        data=self.elasticsearch.search(
            index=mIndex,
            scroll='10m',
            size=chunk_size,
            body=body
        )
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits']) #should be the same as chunk_size
        counter = 0
        result = emptyResult
        while scroll_size > 0:
            #debug
            counter = counter + 1
            if (maxLineCount > 0 and counter > maxLineCount) : exit(0)

            # Before scroll, process current batch of hits
            result = process_data_function(data['hits']['hits'], result, dummy_argument)
            data = self.elasticsearch.scroll(scroll_id=sid, scroll='2m')

            # Update the scroll ID
            sid = data['_scroll_id']

            # Get the number of results that returned in the last scroll
            scroll_size = len(data['hits']['hits'])

        return result

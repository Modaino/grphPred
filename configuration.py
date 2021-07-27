import json

class mConfiguration:
    """Data holder class"""
    def __init__(self):
        self.input_files = {}
        self.input_files['article_file_path'] = "D:/dev/2020/infolegale/rascoussier/J2T/out_2021_07_21.json"
        self.input_files['name_symbol_path'] = ""


        self.ES = {}
        self.ES['connection_initialiser'] = [{'host':'localhost','port':9200}]
        self.ES['index'] = "test_json_input"
        self.ES['doc_type'] = "doc"

        self.ES['english_article_index'] = "all_articles"
        self.ES['english_article_doc_type'] = "doc"

    def toJSON(self):
        return json.dumps(self, default=lambda  o: o.__dict__, sort_keys=True, indent=4)

    def dump(self):
        with open("data_input.config", "w") as configFile:
            configFile.write(self.toJSON())

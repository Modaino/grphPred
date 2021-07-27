from DatabaseHandler import theDatabaseHandler
from configuration import mConfiguration
from spacy_nlp import spacy_nlp
import json
import pandas as pd


config = mConfiguration()
nlp = spacy_nlp()


#ignore \/\/\/\/\/\/\/\/\/\/\/\/\/
def loadScrapedJsonToES():
    dataBaseHandler = theDatabaseHandler(config)
    #dataBaseHandler.init_mapping()
    dataBaseHandler.loadFromJsonToES(config.input_files['article_file_path'], config.ES['index'])

def nlp_analyze_json(jsonPath):
    with open(jsonPath , encoding="utf8") as jsonfile:
        data = json.load(jsonfile)

    nlp = spacy_nlp()
    ner_df =pd.DataFrame(columns=['token','ner_label'])
    i=0
    for jsonLine in data:
        text = jsonLine["full_text"]
        #debug
        print("analyzing text: ", i, ",", jsonLine["url"], ",",jsonLine["title_from_page"])
        i=i+1
        nlp_ok=False
        if (jsonLine["language"]=='fr'):
            doc = nlp.nlp_fr(text)
            nlp_ok=True
        if (jsonLine["language"]=='en'):
            doc = nlp.nlp(text)
            nlp_ok=True

        if nlp_ok:
            for ent in doc.ents:
                ner_df.append({'token': ent.text, 'ner_label':ent.label_}, ignore_index=True)
                print('---',ent.text, '|', ent.label_)

    return ner_df

#end ignore \/\/\/\/\/\/\/\/\/\/\/\/\/

def nlp_analyze_ES_index():
    dataBaseHandler = theDatabaseHandler(config)
    empty_result = pd.DataFrame(columns=['link','token','ner_label'])
    result_df = dataBaseHandler.iterate_whole_es(config.ES['english_article_index'], 10, nlp.analyze_texts_with_nlp_into_df, empty_result, 1)
    print(result_df)

if __name__ == '__main__':
    #loadScrapedJsonToES()
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    nlp_analyze_ES_index()

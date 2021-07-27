import spacy
import pandas as pd

"""
@author:  Elöd Egyed-Zsigmond
you have to download spacy english and french models
under pyCharm (in its venv) the terminal commands  are :
(venv) D:\dev\2020\Scraping\Json2Elastictest>D:\dev\2020\Scraping\Json2Elastictest\venv\Scripts\python -m spacy download en_core_web_trf
(venv) D:\dev\2020\Scraping\Json2Elastictest>D:\dev\2020\Scraping\Json2Elastictest\venv\Scripts\python -m spacy download fr_core_news_md

elsewhere a simple :
python -m spacy download en_core_web_trf
python -m spacy download fr_core_news_md

should work
"""


class spacy_nlp:
    """
    @author:  Elöd Egyed-Zsigmond

    A class do spacy based nlp"""
    def __init__(self):
        self.nlp_en = spacy.load('en_core_web_trf')
        self.nlp_fr = spacy.load('fr_core_news_md')


    def analyze_en_text(self, theText):
        return self.nlp_en(theText)

    def analyze_fr_text(self, theText):
        return self.nlp_fr(theText)

    def analyze_texts_with_nlp_into_df(self, data, previous_result):
        """
        @author:  Elöd Egyed-Zsigmond
        fucntion called when iterating through an ES index to extrat NER-s from the text and append them to a dataframe
        we suppose that texts are in english
        we suppose that they come in a json format containing : title, message and full_text fields.
        actually we analyze the contents of the "message" field  """
        for element in data:
            text = element['_source']["message"]

            #debug
            print("the link : ", element['_source']["link"])

            doc = self.nlp_en(text)
            for ent in doc.ents:
                new_raw = pd.Series({'link': element['_source']["link"],'token': ent.text, 'ner_label':ent.label_})
                previous_result = previous_result.append(new_raw, ignore_index=True)
                #debug
                print('---',ent.text, '|', ent.label_)
        return previous_result

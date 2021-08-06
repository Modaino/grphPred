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
        #self.nlp_fr = spacy.load('fr_core_news_md')


    def analyze_en_text(self, theText):
        return self.nlp_en(theText)

    def analyze_fr_text(self, theText):
        return self.nlp_fr(theText)

    def analyze_texts_with_nlp_into_df(self, data, previous_result, G):
        """
        @author:  Elöd Egyed-Zsigmond
        fucntion called when iterating through an ES index to extrat NER-s from the text and append them to a dataframe
        we suppose that texts are in english
        we suppose that they come in a json format containing : title, message and full_text fields.
        actually we analyze the contents of the "message" field  """
        labels = ["PERSON", "ORG", "LOC", "PRODUCT", "EVENT", "PRODUCT"]

        for element in data:
            text = element['_source']["message"]

            #debug
            #print("the link : ", element['_source']["link"])

            doc = self.nlp_en(text)
            nodes = []
            edges = []
            for ent in doc.ents:
                new_raw = pd.Series({'link': element['_source']["link"],'token': ent.text, 'ner_label':ent.label_})
                previous_result = previous_result.append(new_raw, ignore_index=True)
                #debug
                print('---',ent.text, '|', ent.label_)
                #Collecting possible new nodes
                if ent.label_ in labels:
                    if isAliasUnique(ent.text):
                        nodes.append( (G.number_of_nodes(), {
                            'label': ent.label_,
                            'aliases': [ent.text]
                            }) )
                    else:
                        #TODO: check graph, modifie node attributes and add new edges
                        pass               

            #Generating possible edges
            for node1 in nodes:
                for node2 in nodes:
                    if node1[0] != node2[0]:
                        edges.append(
                            (node1[0], node2[0], {
                                'action' : "",
                                'article_url' : element['_source']["link"],
                                'timestamp' : element['_source']['published'],
                                'weights' : []
                            })                            )

            G.add_nodes_from(nodes)
            G.add_edges_from(edges)


        for elem in nodes:
            print(elem)
        return previous_result

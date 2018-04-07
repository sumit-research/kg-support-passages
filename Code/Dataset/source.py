import pandas as pd 
import csv
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import numpy as np

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")


def get_alias(entity_id):

	sparql.setQuery("""
	SELECT ?altLabel{
	  VALUES (?wd) {(wd:"""+entity_id+""")}
	 ?wd skos:altLabel ?altLabel .
	 FILTER (lang(?altLabel) = "en")
	}""")
	sparql.setReturnFormat(JSON)
	results = sparql.query().convert()
	results_df = pd.io.json.json_normalize(results['results']['bindings'])
	return results_df
    #print(results_df)

def write_file(row,entity):
	rel_id = str(row[['wdt.value']].values[0]).split('/')[-1]
	rel_name = str(row[['wdLabel.value']].values[0]).split('/')[-1]
	#print('Rel:',rel_name)
	if(rel_id=='P31' or rel_id=='P279'):
		return
	rel_alias = get_alias(rel_id)
	#print(rel_alias)
	if(rel_alias.shape[1]!=3 or rel_alias.shape[0] < 1):
		return


	#Choose only english
	rel_alias = rel_alias[rel_alias['altLabel.xml:lang']=='en']
	alias_list = rel_alias[['altLabel.value']].values

	relationship = row[['wdLabel.value']].values[0]
	second_entity = row[['ooLabel.value']].values[0]
	#print('Relationship:',relationship)
	
	if(alias_list.size==0):
		f1.write(entity+"**"+relationship+"**"+second_entity+'\n')
		f1.flush()
	else:
		alias_list = list(alias_list.flat)
		alias_list.reverse()
		alias_list.append(relationship)
		alias_list.reverse()
		single_alias_list = '&'.join(alias_list)
		f1.write(entity+"**"+single_alias_list+"**"+second_entity+"\n")
		f1.flush()
		#print(single_alias_list)



def sparql_query(entity,entity_id):
    sparql.setQuery("""
    SELECT ?wdt ?wdLabel ?ooLabel
    WHERE {
     VALUES (?s) {(wd:"""+entity_id+""")}
     ?s ?wdt ?o .
     ?wd wikibase:directClaim ?wdt .
     ?wd rdfs:label ?wdLabel .
     OPTIONAL {
     ?o rdfs:label ?oLabel .
     FILTER (lang(?oLabel) = "en")
     }
     FILTER (lang(?wdLabel) = "en")
     BIND (COALESCE(?oLabel, ?o) AS ?ooLabel)
     } ORDER BY xsd:integer(STRAFTER(STR(?wd), "http://www.wikidata.org/entity/P"))
    """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    results_df = pd.io.json.json_normalize(results['results']['bindings'])
    if(len(list(results_df))==0):
    	return

    #print(results_df[['wdt.value']])
    #triple images----------------------------------------- 
    #select only english----------------------------------
    results_df = results_df[results_df['ooLabel.xml:lang']=='en']
    results_df = results_df[results_df['wdLabel.xml:lang']=='en']
    #print(results_df)
    results_df = results_df.filter(['ooLabel.value','wdt.value','wdLabel.value'], axis=1)
    #new_results_df = new_results_df[pd.notnull(results_df['ooLabel.xml:lang'])]
    #new_results_df.columns = ['Entity','LabelType' ,'Relationship']
    #new_results_df = new_results_df.filter(['Relationship','Entity'], axis=1)
    #print(new_results_df)
    #results_df = results_df.dropna(subset = ['ooLabel.xml:lang'])
    #results_df = results_df.dropna(subset = ['wdLabel.xml:lang'])

    #Now extract synonyms from dataset
    results_df.apply(lambda row: write_file(row,entity),axis = 1) # equiv to df.sum(0)
    
    #df.apply(lambda row: EOQ(row['D'], row['p'], ck, ch), axis=1)
    #print(results_df)
#    wdLabel.xml:lang
    return(results_df)





def get_entity_id(entity):
	sparql.setQuery("""
	SELECT ?item WHERE {
	  ?item rdfs:label \""""+entity+"""\"@en.
	}""")
	sparql.setReturnFormat(JSON)
	results = sparql.query().convert()
	
	results_df = pd.io.json.json_normalize(results['results']['bindings'])
	if(results_df.empty):
		return
	else:
		np_df = results_df.as_matrix()
		top_id = np_df[0][1].split('/')[-1]
		result_dataframe = sparql_query(entity,top_id)
		#print(result_dataframe)
		

if __name__=='__main__':
	f1 = open('triples.txt','w')
	with open('entity.txt','r') as f:
		lines = f.readlines()
		for i,line in enumerate(lines):
			print('ID:',i)
			entity_term = line.strip()
			get_entity_id(entity_term)
	f1.close()
			
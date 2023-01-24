#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 11:00:40 2023

@author: Pablo
"""




from SPARQLWrapper import SPARQLWrapper, JSON


endpoint= "https://fuseki.pcalleja.linkeddata.es/UNE"

def query():
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setQuery("""
        select ?bp (count(?p) as ?num_politicians) 
        WHERE {
            ?p a <http://dbpedia.org/ontology/Politician>.
            ?p <http://dbpedia.org/ontology/birthPlace> ?bp.
            ?bp a <http://dbpedia.org/ontology/Country> 
        }group by ?bp order by desc (?num_politicians) limit 10
    """)
    
    try:
        for result in sparql.queryAndConvert()["results"]["bindings"]:
          print(result['bp']['value'],result['num_politicians']['value'])
    except Exception as e:
        print(e)
        
        
        
        
def query_for_entity(entity):
    
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setQuery("""
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        PREFIX une: <http://une.linkeddata.es/scheme/>
        
        SELECT     ?texttitle ?price
        WHERE {
          ?book une:hasTitle ?texttitle .
          ?book une:price ?price .
          ?book ?r ?e .
          ?e une:spanText \""""+entity+"""\" .
         
        } GROUP BY ?texttitle  ?price
        LIMIT 1000
    """
    )
    #print(sparql.queryAndConvert())
    try:
        for result in sparql.queryAndConvert()["results"]["bindings"]:
          #print(result)
          print(result['texttitle']['value'],result['price']['value'])
    except Exception as e:
        print(e)
        
        
        
def get_context_of_entity(entity):
    query= """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX une: <http://une.linkeddata.es/scheme/>
    SELECT    ?init ?end  ?ent
    WHERE {
    
    
      ?ent une:spanText \""""+entity+"""\"  .
      ?ent une:init ?init .
      ?ent une:end ?end .
      ?ent une:type une:type\/person .
      #?book une:hasEntity ?ent.
      
      #?ent une:section ?para.
    
      
    }
    LIMIT 10000
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    #print(sparql.queryAndConvert())
    try:
        for result in sparql.queryAndConvert()["results"]["bindings"]:
          #print(result)
          print(result['init']['value'],result['end']['value'], result['ent']['value'] )
          
          init= result['init']['value']
          end = result['end']['value']
          
          iden= result['ent']['value']
          #http://une.linkeddata.es/scheme/entity/9788417633691/1/43/301_322
          iden = iden.replace('http://une.linkeddata.es/scheme/entity/','')
          splited= iden.split('/')
          iden= '_'.join(splited[:-1])
          iden = iden.replace('/','_')
          print(iden)
          
          get_text_from_paragraph(iden,init,end)
    except Exception as e:
        print(e)


def get_text_from_paragraph(identifier, init,end):
    
    init = int(init)
    end= int(end)
    doc = getDocumentFromIndex(es,'paragraph',identifier)
    text= doc['_source']['text']
    
    init_mark= int(init)-50
    end_mark= int(end)+50
    if init_mark <0:
        init_mark=0
    if end_mark> len(text)-1:
        end_mark=len(text)
    print(text[init_mark:init]+'|'+text[init:end]+'|'+text[end:end_mark])
    #print(text)

from UNE_elastic import getLocalConnection, getDocumentFromIndex
es = getLocalConnection()      

#lista= getAllDocumentsFromIndex(es, 'section')



import os
get_context_of_entity('Federico García Lorca') 
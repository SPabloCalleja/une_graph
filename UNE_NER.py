#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 11:37:36 2022

@author: Pablo
"""


import os

import spacy
from transformers import pipeline
import sys
#!pip install spacy
#!pip uninstall spacy -y
#!python -m spacy download es_core_news_sm
#!pip uninstall transformers -y
#!pip install transformers

#!pip uninstall keras -y
#!pip install keras
#!pip uninstall tensorflow -y
#!pip install tensorflow

nlp = spacy.load("es_core_news_sm")

def get_sentences(text):
        
    doc = nlp(text)
    ls=[]
    for s in doc.sents:
        ls.append(s)
    return ls
    
   


def ner_text(text):
    
    set_persons=[]
    set_locations=[]
    set_organizations=[]
    set_misc=[]
    
    global nlp_ner
    general_offset=0
    sentences= get_sentences(text)
    for sent in sentences:
        #print(sent)
        try:
            entities= nlp_ner(str(sent))
            
            for entity in entities:
                #print(entity)
                
                start_position = int(entity['start']) + general_offset
                end_position=    int(entity['end']  ) + general_offset
                entity_text= entity['word']+'['+str(start_position)+':'+str(end_position)+']'
                group = entity['entity_group']
                if group == 'ORG':
                    set_organizations.append(entity_text)
                if group == 'LOC':
                    set_locations.append(entity_text)
                
                if group == 'PER':
                    set_persons.append(entity_text)
                if group == 'MISC':
                    set_misc.append(entity_text)
            ##update offsets
            general_offset=general_offset+len(sent)
        except:
            general_offset=general_offset+len(sent)
            continue
    
    return set_persons,set_locations,set_organizations,set_misc



def prepare_row(doc_id,section_id,parr_id, per_entities, loc_entities, org_entities,misc_entities):
    
    row=[doc_id,section_id,parr_id, '|'.join(per_entities), '|'.join(loc_entities), '|'.join(org_entities),'|'.join(misc_entities)]
    return row


def write_tsv(name,content):
    with open(name, "w", encoding='utf8') as write_file:
        for row in content:
            write_file.write('\t'.join(row))
            write_file.write('\n')
            
            
        

nlp_ner = pipeline(
"ner",
model="mrm8488/bert-spanish-cased-finetuned-ner",
tokenizer=(
    'mrm8488/bert-spanish-cased-finetuned-ner',  
     {"use_fast": True},
    
),
 aggregation_strategy= 'max'
)


def process_txt_file(file,filename):
    
    file_id= filename.replace('.txt','').split('_')[0]
    section_id= filename.replace('.txt','').split('_')[1]
    par_counter=0
    rows=[]
    f = open(file,encoding='utf-8')     # This is a big file
    for line in f:                # Using 'for ... in' on file object
        
        res1,res2,res3,res4=ner_text(line)
        row= prepare_row(file_id,section_id,str(par_counter),res1,res2,res3,res4)
        rows.append(row)
    
        par_counter+=1

    f.close()
    
    return rows

    
def process_txt_folder(folder_name,output_folder):

    total_rows=[]
    counter=0
    for (root, dirs, files) in os.walk(folder_name):
        total=len(files)
        for f in files:
            
            if  f.endswith('.txt' ):
                print(f)
                print((counter/total)*100)
                try:
                    rows= process_txt_file(os.path.join(root, f),f)
                    total_rows.extend(rows)
                    
                    
                except  Exception as e:
                    print('Error in: '+f+str(e))
                    #break
            counter+=1
            #if counter>4:
                #break
            #break
    write_tsv(os.path.join(output_folder, 'UNE_ner_results_offsets.tsv'),total_rows)


def main(argv):
    input_folder = argv[0]
    output_folder = argv[1]
    
    
    process_txt_folder(input_folder,output_folder)





if __name__ == '__main__':
    main(sys.argv[1:])


  



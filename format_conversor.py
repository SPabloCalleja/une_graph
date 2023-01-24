#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 13:13:23 2023

@author: Pablo
"""





import json
from bs4 import BeautifulSoup 
import logging

import sys

'''
def remove_bib(element):
    for e in element.find_all('ref'):
        e.decompose()
    return element

def remove_figures(element):
    for e in element.find_all('figure'):
        e.decompose()
    return element

def remove_formula(element):
    for e in element.find_all('formula'):
        e.decompose()
    return element
'''
def remove_tag(element,tag):
    for e in element.find_all(tag):
        e.decompose()
    return element



def clean_element(element):
    element= remove_tag(element,'ref')
    element= remove_tag(element,'figure')
    element= remove_tag(element,'formula')
    return element


###




def get_sections(element):
    
    try:
        body = element.find('body') 
    
        secciones = body.find_all('div') 
        
        
        sect_json=[]
       
        for sec in secciones:
            
            section_json={}
            
            head= sec.find('head')
            if head != None:
                section_json['head']=head.text
            
                
            
            paragraphs = sec.find_all('p')
            p_json=[]
            
            
            for p in paragraphs:
                p= clean_element(p)
                p_json.append(p.text)
            
            section_json['p']=p_json
            sect_json.append(section_json)
        
        return sect_json
    except Exception as e:
        logging.error('Error extracting sections '+e)
        print('Error extracting sections')
    

    return []
    

    

def get_authors(element):
    authors={}
    lis_authors=[]
    authors = element.find('teiHeader').find_all('persName') 
    for a in authors:
        name = a.find('forename')
        surname= a.find('surname')
        if name == None:
            name=''
        else:
            name=name.text
        if surname == None:
            surname=''
        else:
            surname=surname.text
        lis_authors.append(name+' '+surname)
    #authors['authors']=lis_authors
    return lis_authors

def get_abstract(element):
    
    try:
        # Finding all instances of tag   
        b_unique = element.find_all('abstract') 
        if b_unique== None:
            return []
        b_text = b_unique[0].find('div')
        
        if b_text== None:
            return []
        parr=[]
        paragraphs = b_text.find_all('p')
        for p in paragraphs:
            p = clean_element(p)
            parr.append(p.text)
        
        return parr
        
    except Exception as e:
        logging.error('Error creating header '+e)
        print('No abstract')
    

    return []


def get_title(element):
    
    title= ''
    try:
        # Finding all instances of tag   
        e_title = element.find('titleStmt')
        
        if e_title == None:
            return ''
        
        e2 = e_title.find('title')
        if e2 == None:
            return ''
        
        title = str(e2.text)
        
        
        
    except Exception as e:
        logging.error('Error creating title '+e)
        print('No title')
    

    return title
   

def write_json_paper(file,content):
    with open(file, "w", encoding='utf8') as write_file:
        json.dump(content, write_file, indent=4,ensure_ascii=False)

def process_tei_file(file):
    # Reading the data inside the xml file to a variable under the name  data
    with open(file, 'r') as f:
        data = f.read() 
    
    
    file_id = file.split('/')[-1].replace('.tei.xml','')
    
    my_json={}
    
    # Passing the stored data inside the beautifulsoup parser 
    bs_data = BeautifulSoup(data, 'xml')
    
    
    my_json['id']=file_id
    
    ## title
    my_json['title']= get_title(bs_data)
    
    
    ## abstract
    parr_abstract=get_abstract(bs_data)
    
    #print(my_json)
    abst_p={}
    abst_p['p']= parr_abstract
    my_json['abstract']=abst_p
    
    
    ## sections
    my_json['sections']=get_sections(bs_data)
    
    
    '''
    try:
        my_json['authors']=get_authors(bs_data)
    except Exception as e:
        logging.error('Error creating authors '+e)
        raise Exception("Error")
    
    '''
    return my_json





def valid_entity(entity):
    
    if len(entity)<=3 and not entity.isupper():
        
       return False 
        
        
    return True

'ner[0:3]'.split('[')[1][:-1]

def preprocess_entity(entity):
    
    if '\'' in entity:
        entity=entity.replace('\'',' ')
    if '\"' in entity:
        entity=entity.replace('\"',' ')
    return entity

def prepare_lines_of_entity_group(book_id,section, paragraph, entities,group):
    
    lis_entities= entities.split('|')
    lines=[]
    for ent in lis_entities:
        
        ent_text= ent.split('[')[0]
        ent_offset= ent.split('[')[1][:-1]
        
        #print(ent)
        print(ent_text)
        #print(ent_offset)
        
        init = ent_offset.split(':')[0]
        end = ent_offset.split(':')[1]
        
        if not valid_entity(ent_text):
            continue
        
        line= [book_id,section,paragraph,preprocess_entity(ent_text),init,end,group]
        lines.append('\t'.join(line))
        
    return lines    
    


def write_output_file(file,content):
    with open(file, "w", encoding='utf8') as write_file:
        for c in content:
            write_file.write(str(c)+'\n')


def transform_ner_result_for_triple_dataset(input_file,output_file):
    
    with open(input_file, 'r') as f:
        data = f.readlines()
        
    dataset_lines=[]
    dataset_lines.append('book_id\tsection\tparagraph\ttext\tinit\tend\ttype')
    for line in data:
        
        try:
            line= line.replace('\n', '')
            #print(line)
            segments= line.split('\t')
            
            book= segments[0]
            section= segments[1]
            paragraph= segments[2]
            person_entities= segments[3]
            loc_entities= segments[4]
            org_entities= segments[5]
            misc_entities= segments[6]
            
            if len(person_entities) > 1:
                dataset_lines.extend(prepare_lines_of_entity_group(book,section,paragraph,person_entities,'person'))
            if len(loc_entities) > 1:
                dataset_lines.extend(prepare_lines_of_entity_group(book,section,paragraph,loc_entities,'location'))
            if len(org_entities) > 1:
                dataset_lines.extend(prepare_lines_of_entity_group(book,section,paragraph,org_entities,'organization'))
            if len(misc_entities) > 1:
                dataset_lines.extend(prepare_lines_of_entity_group(book,section,paragraph,misc_entities,'miscellaneous'))
            #set_persons,set_locations,set_organizations,set_misc
        
        except  Exception as e:
            print(e)


    write_output_file(output_file,dataset_lines)








def main(argv):
    input_file = argv[0]
    output_file = argv[1]
    
    logging.basicConfig(filename='UNE_tei_extractor.log', level=logging.INFO)
    logging.info('Started')
    transform_ner_result_for_triple_dataset(input_file,output_file)
    logging.info('Finished')

if __name__ == '__main__':
    main(sys.argv[1:])

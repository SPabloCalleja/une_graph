# -*- coding: utf-8 -*-





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



def write_txt_content(file,content):
    with open(file, "w", encoding='utf8') as write_file:
        for c in content:
            write_file.write(str(c)+'\n')

def output_segmented_txt(json_file,  path):
    
    #os.path.join(output_folder,f+'.json')
    
    iden= json_file['id']
    var=0
    for sect in json_file['sections']:
        
        text=[]
        if 'head' in sect:
            text.append(sect['head'])
        for p in sect['p']:
            text.append(p)
        
        write_txt_content(os.path.join(path,str(iden)+'_'+str(var)+'.txt'),text)    
        var=var+1




import os
def process_tei_folder(folder_name,output_folder):

    # dirs=directories
    for (root, dirs, files) in os.walk(folder_name):
        for f in files:
            
            if  f.endswith('.tei.xml' ):
                print(f)
                try:
                    #path=    '/Users/Pablo/Downloads/REDIB_SML/training_tips.tei.xml' 
                    json_file= process_tei_file(os.path.join(root, f))
                    #print(json_file)
                    
                    output_segmented_txt(json_file,output_folder)
                    
                except  Exception as e:
                    logging.error('Error in: '+f+' '+str(e))
                    print('Error in: '+f+str(e))
                    #break
                    
            #break





def main(argv):
    input_folder = argv[0]
    output_folder = argv[1]
    
    logging.basicConfig(filename='UNE_tei_extractor.log', level=logging.INFO)
    logging.info('Started')
    process_tei_folder(input_folder,output_folder)
    logging.info('Finished')

if __name__ == '__main__':
    main(sys.argv[1:])

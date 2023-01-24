from parsr_client import ParsrClient as clientfrom time import sleepimport reimport loggingfrom PyPDF2 import PdfWriter, PdfReaderimport sysimport osimport pandas as pddef get_ids(folder_name):    lista=[]    # dirs=directories    for (root, dirs, files) in os.walk(folder_name):        for f in files:                        if  f.endswith('.xml' ):                lista.append(f.replace('.tei.xml',''))    return lista                                    import iodef write_file(name,content):    with io.open(name,'w',encoding='utf8') as f:        f.write(content)        def read_file(name):    with io.open(name,'r',encoding='utf8') as f:        return f.read()def main(argv):    input_folder = argv[0]    output_folder = argv[1]    if len(argv) >2:        mode = argv[2]    else:        mode='normal'        #logging.basicConfig(filename='parsr_extractot.log', level=logging.INFO)    #logging.info('Started')    # convert_folder(input_folder,output_folder,mode)    #logging.info('Finished')        def csv_creator():     #main(sys.argv[1:])    data= pd.read_csv('Ebooks-20220916.csv',sep=';',encoding='utf-8')        group='UGR'        lis=  get_ids('CorpusUNE/'+group+'_XML')    counter=0    for index, row in data.iterrows():        ids= row['EAN']        if ids in lis:            counter=counter+1    print(counter)        data = data[data['EAN'].isin(lis)]    data = data.reset_index(drop=True)  # make sure indexes pair with number of rows    data.to_csv(group+'.csv', sep=';',encoding='utf-8')    data2 = data.dropna(subset=['Sinopsis'],axis=0)    data2 = data2.drop([ 'Estado','Precio','Gratuito','Fecha de venta','Sello Editorial','Fecha de edición','Páginas','Número de edición','Traductor','Código postal'], axis=1)    data2 = data2.reset_index(drop=True)      data2.to_csv(group+'_summary.csv', sep=';',encoding='utf-8')    def read_full_text(file_path):    with open(file_path, encoding='utf-8') as fh:        data = json.load(fh)        return data['body']    import jsonif __name__ == '__main__':            #;EAN;Título;Subtítulo;Autor;Sinopsis;Materias Ibic    jsonl=[]    data= pd.read_csv('PUZ_summary.csv',sep=';',encoding='utf-8')    for index, row in data.iterrows():        ids= row['EAN']        fi='CorpusUNE/PUZ_text/'+str(ids)+'.tei.xml.json'        if os.path.exists(fi):            txt= read_full_text(fi)            jsonrow={}            jsonrow['id']= str(ids)            jsonrow['title']= str(row['Título'])            jsonrow['summary']= str(row['Sinopsis'])            jsonrow['text']= txt            jsonl.append(jsonrow)                                            else:            print('not found'+str(ids))    data= pd.read_csv('UGR_summary.csv',sep=';',encoding='utf-8')    for index, row in data.iterrows():        ids= row['EAN']        fi='CorpusUNE/UGR_text/'+str(ids)+'.tei.xml.json'        if os.path.exists(fi):            txt= read_full_text(fi)            jsonrow={}            jsonrow['id']= str(ids)            jsonrow['title']= str(row['Título'])            jsonrow['summary']= str(row['Sinopsis'])            jsonrow['text']= txt            jsonl.append(jsonrow)                                            else:            print('not found'+str(ids))    import io            with io.open('UNE_summaries.jsonl','w',encoding='utf8') as f:        for l in jsonl:            json.dump(l,f,ensure_ascii=False)            f.write('\n')            
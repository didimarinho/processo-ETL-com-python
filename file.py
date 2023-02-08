import pandas as pd
import glob #! este módulo ajuda na seleção de arquivos
import xml.etree.ElementTree as ET #! este módulo ajuda no processamento de arquivos XML
from datetime import datetime



tmpfile    = "temp.tmp"               # file used to store all extracted data
logfile    = "logfile.txt"            # all event logs will be stored in this file
targetfile = "transformed_data.csv"   # file where transformed data is stored


#! organizando os arquivos csv em data frame
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe
    
    
    


#! organizando os arquivos json em data frame
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe
    



#! organizando os arquivos xml em data frame
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name":name, "height":height, "weight":weight}, ignore_index=True)
    return dataframe
    




#! criando um adata frame vazio com as colunas 'name', 'height', e 'weight' e extraindo as info
#! dos arquivos csv, json e xml
def extract():
    extracted_data = pd.DataFrame(columns=['name','height','weight'])
    
    #! glob vai retornar uma lista com todos docs tipo csv no diretório
    for csvfile in glob.glob("*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
        
    #! glob vai criar uma lista com todos docs tipo json no diretório
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    
    #! glob vai criar uma lista com todos docs tipo xml no diretório
    for xmlfile in glob.glob("*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data
   



#! convertendo as unidades de medidas de um dataframe
def transform(data):
        # converte de polegadas para metros
        data['height'] = round(data.height * 0.0254,2)
        
        
        # converte de libras para kilogramas
        data['weight'] = round(data.weight * 0.45359237,2)
        return data
      



def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)  
    

#! Criando um arquivo txt com os logs
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')    
        
        
        
log("ETL Job Started")

log("Extract phase Started")
extracted_data = extract()

log("Extract phase Ended")
extracted_data

log("Transform phase Started")
transformed_data = transform(extracted_data)

log("Transform phase Ended")
transformed_data 

log("Load phase Started")
load(targetfile,transformed_data)


log("Load phase Ended")

log("ETL Job Ended")
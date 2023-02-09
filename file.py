import pandas as pd
import glob #! este módulo ajuda na seleção de arquivos
import xml.etree.ElementTree as ET #! este módulo ajuda no processamento de arquivos XML
from datetime import datetime






'''
VAMOS CRIAR UMA FUNÇÃO STANDARD DE LEITURA DE ARQUIVO CSV QUE VAMOS USAR
COMO PARTE DE OUTRA FUNÇÃO
'''
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe
    
    



'''
VAMOS CRIAR UMA FUNÇÃO STANDARD DE LEITURA DE ARQUIVO JSON QUE VAMOS USAR
COMO PARTE DE OUTRA FUNÇÃO
'''
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe
    



'''
VAMOS CRIAR UMA FUNÇÃO STANDARD DE LEITURA DE ARQUIVO XML QUE VAMOS USAR
COMO PARTE DE OUTRA FUNÇÃO
'''
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
    



'''
VAMOS CRIAR UM DATA FRAME VAZIO COM AS COLUNAS NAME, HEIGHT E WEIGHT E VAMOS USAR AS FUNÇÕES QUE CRIAMOS 
ACIMA PARA EXTRAIR OS ARQUIVOS E ISERIR NO DATA FRAME VAZIO CHAMADO EXTRACTED_DATA
'''
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
   



'''
VAMOS CRIAR UMA FUNÇÃO STANDART PARA USAR MAIS TARDE QUE VAI TRANSFORMAR AS MEDIDAS 
'''
def transform(data):
        # converte de polegadas para metros
        data['height'] = round(data.height * 0.0254,2)
        
        
        # converte de libras para kilogramas
        data['weight'] = round(data.weight * 0.45359237,2)
        return data
      


'''
VAMOS CRIAR UMA FUNÇÃO STANDARD PARA CRIAR UM ARQUIVO CSV EM NOSSO 
DIRETÓRIO COM AS INFORMAÇÕES DAS TRANSFORMAÇÕES AUTOMATICAMENTE
'''
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)  



 
'''
VAMOS CRIAR OS LOGS DE INFORMAÇÃO DE CADA PROCESSO DO ETL
'''
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')    




'''
LOG DE INICIO DO PROCESSO ETL
'''       
log("ETL Job Started")



'''
VAMOS COMEÇAR A EXTRAÇÃO DOS DADOS
'''
log("Extract phase Started")
extracted_data = extract()



'''
INFORMAÇÃO DE EXTRÇÃO ATRAVÉS DO LOG
'''
log("Extract phase Ended")
extracted_data


'''
TRANSFORMANDO OS DADOS DE HEIGHT E WEIGHT
'''
log("Transform phase Started")
transformed_data = transform(extracted_data)

'''
INFORMAÇÃO DE LOG DA TRANSFORMAÇÃO DOS DADOS
'''
log("Transform phase Ended")
transformed_data 


'''
VAI DAR INFORMAÇÃO DE LOG E VAI CRIAR O ARQUIVO CSV
COM OS DADOS TRANSFORMADOS E CARREGADOS
'''
log("Load phase Started")
load(targetfile,transformed_data)



'''
INFORMAÇÃO DE LOG PARA FINALIZAÇÃO DO ETL
'''
log("Load phase Ended")
log("ETL Job Ended")
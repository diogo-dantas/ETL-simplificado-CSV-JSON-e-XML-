import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

''' Dois arquivos de saída: transform_data.csv para armazenar os dados finais que você pode carregar em um banco de dados,
 e log_file.txt, que armazenará todos os logs.'''

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

 função para ler os arquivos em formato csv e retornar em dataframe

def extract_from_csv(file_to_process): 
	dataframe = pd.read_csv(file_to_process) 
	return dataframe 

# função para ler os arquivos em formato json e retornar em dataframe

def extract_from_json(file_to_process): 
    dataframe = pd.read_json(file_to_process, lines=True) 
    return dataframe 

# função para ler os arquivos em formato xml e retornar em dataframe

def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name,"height":height, "weight":weight}])], ignore_index=True) 
    
    return dataframe 
# Criando um dataframe vazio para armazenar os dados extraídos

def extract(): 
    extracted_data = pd.DataFrame(columns=['name','height','weight']) 
     
# armazena as infos do arquivo csv para o dataframe criado
    for csvfile in glob.glob("*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
             
# armazena as infos do arquivo json para o dataframe criado
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
         
# armazena as infos do arquivo xml para o dataframe criado
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
             
    return extracted_data 
    
'''Converte polegadas em metros e arredonda para duas casas decimais. 
1 polegada equivale a 0,0254 metros 
Converte libras em quilogramas e arredonda para duas casas decimais. 
1 libra equivale a 0,45359237 quilogramas '''    

def transform(data): 
    data['height'] = round(data.height * 0.0254,2) 
    data['weight'] = round(data.weight * 0.45359237,2) 
    return data 

# Carrega o o arquivo transformado para o arquivo csv a ser carregado em um banco de dados

def load_data(target_file, transformed_data): 
    transformed_data.to_csv(target_file) 

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')     
 	

# Log de inicialização.
log_progress("ETL Job Started") 
 
# Log para o início do processo de extração.
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Extração completa
log_progress("Extract phase Ended") 
 
# Inicio da transformação dos dados.
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Processo de transformação completo. 
log_progress("Transform phase Ended") 
 
# Inicio de processo de carga dos dados
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Carga completa. 
log_progress("Load phase Ended") 
 
# Processo de ETL concluído.
log_progress("ETL Job Ended")

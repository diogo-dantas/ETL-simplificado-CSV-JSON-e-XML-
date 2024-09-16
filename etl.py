import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

''' Dois arquivos de saída: transform_data.csv para armazenar os dados finais que você pode carregar em um banco de dados,
 e log_file.txt, que armazenará todos os logs.'''

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

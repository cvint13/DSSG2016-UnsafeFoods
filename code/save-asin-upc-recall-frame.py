from urllib.request import urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup
import re

import sys
import os
sys.path.append('.')

print(os.getcwd())
import data_preprocessing as dp

import pandas
recall_data = pandas.read_csv("data/enforce_upcs.csv")

for row in range(recall_data.shape[0]):
    upcs = str(recall_data.upc[row]).split(';')
    asins = []
    upcs = list(set(upcs))
    for upc in upcs:
        print(upc)
        upc = str(upc).strip()
        asins.append(dp.UPCtoASIN(upc))
    recall_data.asins[row] = asins
	
recall_data.to_csv('data/recalls_upcs_asins_joined.csv')
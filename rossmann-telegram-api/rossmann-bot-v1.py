#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
import json

#carregando dados de teste
df10 = pd.read_csv(r'C:\Users\laism\Downloads\rossmann-store-sales\test.csv')
df_store_raw = pd.read_csv(r'C:\Users\laism\Downloads\rossmann-store-sales\store.csv', low_memory=False)

#merge do dataset testes + store
df_test = pd.merge(df10, df_store_raw, how='left', on = 'Store')

#escolhendo a loja para predição
#df_test = df_test[df_test['Store'].isin([24,12,22])]
df_test = df_test[df_test['Store'] == 22]

#remove dias fechados
df_test = df_test[df_test['Open'] != 0]
#removendo lojas vazias
df_test = df_test[~df_test['Open'].isnull()]
#apagando a coluna id
df_test = df_test.drop('Id', axis=1)

#convertendo df para Json
data = json.dumps(df_test.to_dict(orient = 'records'))

# API Call
#url é o end point, pra onde eu vou enviar o pedido
#url = 'http://127.0.0.1:5000/rossmann/predict'
#url = 'http://0.0.0.0:5000/rossmann/predict'
#url = 'http://192.168.0.214:5000/rossmann/predict'
url = 'https://rossmann-api-u23h.onrender.com/rossmann/predict'

#header indica para ip que tipo de dado ela está recebendo
header = { 'Content-type': 'application/json'}

data = data

#post é um metodo para enviar dados
r = requests.post(url, data=data, headers=header)
print('Status Code {}'.format(r.status_code))

#transformando o resultado em dataframe
df1 = pd.DataFrame( r.json(), columns = r.json()[0].keys())

#mostrando o valor de vendas no final das 6 semanas
d2 = df1[['store', 'prediction']].groupby( 'store' ).sum().reset_index()

for i in range( len( d2 ) ):
    print( 'Store Number {} will sell R${:,.2f} in the next 6 weeks'.format( 
            d2.loc[i, 'store'], 
            d2.loc[i, 'prediction'] ) )


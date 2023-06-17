#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import os
import pickle 
import pandas as pd
from flask import Flask, request
from rossmann.Rossmann import Rossmann

#carregando o modelo
model = pickle.load( open('model\model_rossmann.pkl', 'rb'))

#inicializando a API
app = Flask(__name__)

#criando o end point
#POST é o metodo que envia dados 
@app.route('/rossmann/predict', methods=['POST'])

#quando o end point recebe uma chamada vida post, ele executa a primeira função abaixo dele

def rossmann_predict():
    test_json = request.get_json()
    
    if test_json: #se tem dado, converte json em dataframe
        if isinstance(test_json, dict): #exemplo unico
            test_raw = pd.DataFrame(test_json, index=[0])
        else: #exemplo multiplos
            test_raw = pd.DataFrame(test_json, columns=test_json[0].keys())
        
        #instanciando a classe rossmann
        pipeline = Rossmann()
        
        #data cleaning
        df1 = pipeline.data_cleaning(test_raw)
        
        #feature enginnering
        df2 = pipeline.feature_engineering(df1)
        
        #data preparation
        df3 = pipeline.data_preparation(df2)
        
        #prediction
        df_response = pipeline.get_prediction(model, test_raw, df3)
        
        return df_response

        
    else: #se não tem dado
        return Response('{}', status=200, mimetype='application/json')

if __name__ == '__main__':
    port = os.environ.get( 'PORT', 5000)
    #'0.0.0.0' end point do local host
    app.run(host = '0.0.0.0', port = port)








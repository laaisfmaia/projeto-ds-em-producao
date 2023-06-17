import pandas as pd
import requests
import json
from flask import Flask,request, Response

#constants
token = '6070726420:AAGIzDDjcTKLDDnYv8JnfRPklSNsxiJyEao'

#modelo de url
#https://api.telegram.org/bot<token>/METHOD_NAME
    
#info do bot
#https://api.telegram.org/bot6070726420:AAGIzDDjcTKLDDnYv8JnfRPklSNsxiJyEao/getMe

#get updates -> consegue pegar a mensagem que envia pelo telegram
#https://api.telegram.org/bot6070726420:AAGIzDDjcTKLDDnYv8JnfRPklSNsxiJyEao/getUpdates
        
#send message
#chat id pega da get updates
#https://api.telegram.org/bot6070726420:AAGIzDDjcTKLDDnYv8JnfRPklSNsxiJyEao/sendMessage?chat_id=6216511342&text=Hi Lais

#setWebhook link a url do telegram para algum end point
#https://api.telegram.org/bot6070726420:AAGIzDDjcTKLDDnYv8JnfRPklSNsxiJyEao/setWebhook?url=https://admin.localhost.run/
#https://api.telegram.org/bot6070726420:AAGIzDDjcTKLDDnYv8JnfRPklSNsxiJyEao/setWebhook?url=https://33e2fcfd7dd02e.lhr.life
        
 
    
    
def send_message(chat_id, text):
    url = 'https://api.telegram.org/bot{}/'.format(token)
    url = url + 'sendMessage?chat_id={}'.format(chat_id) 
    
    r = requests.post(url, json={'text': text})
    print('Status Code {}'.format(r.status_code))
    
    return None
      
def load_dataset(store_id):
    #carregando dados de teste
    df10 = pd.read_csv(r'C:\Users\laism\Downloads\rossmann-store-sales\test.csv')
    df_store_raw = pd.read_csv(r'C:\Users\laism\Downloads\rossmann-store-sales\store.csv', low_memory=False)

    #merge do dataset testes + store
    df_test = pd.merge(df10, df_store_raw, how='left', on = 'Store')

    #escolhendo a loja para predição
    #df_test = df_test[df_test['Store'].isin([24,12,22])]
    df_test = df_test[df_test['Store'] == store_id]
    
    if not df_test.empty:
        #remove dias fechados
        df_test = df_test[df_test['Open'] != 0]
        #removendo lojas vazias
        df_test = df_test[~df_test['Open'].isnull()]
        #apagando a coluna id
        df_test = df_test.drop('Id', axis=1)

        #convertendo df para Json
        import json
        data = json.dumps(df_test.to_dict(orient = 'records'))
    
    else:
        data = 'error'
    
    return data

def predict( data):
    # API Call
    url = 'https://rossmann-api-u23h.onrender.com/rossmann/predict'

    #header indica para ip que tipo de dado ela está recebendo
    header = { 'Content-type': 'application/json'}

    data = data

    #post é um metodo para enviar dados
    r = requests.post(url, data=data, headers=header)
    print('Status Code {}'.format(r.status_code))

    #transformando o resultado em dataframe
    df1 = pd.DataFrame( r.json(), columns = r.json()[0].keys())
    
    return df1

def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']
    
    store_id.replace('/','')
    
    #tentando converter o numero em texto
    try:
        store_id = int(store_id)
    except ValueError:
        store_id_= 'error'
        
    return chat_id, store_id


#criando o end point 
#instanciando o flask / inicializando a API
#o telegram vai enviar msg para esse end point e pego a msg daqui
app = Flask(__name__)


#criando o end point / a rota onde a msg vai chegar
@app.route('/', methods=['GET', 'POST'])

#função que roda toda vez que o end point for acionado 
def index():
    if requests.method == 'POST':
        message = requests.get_json()
        
        chat_id, store_id = parse_message(message)
        if store_id != 'error':
            #loading data
            data = load_dataset( store_id)
            
            if data != 'error':
                #prediction
                df1 = prediction (data)

                #calculation
                #mostrando o valor de vendas no final das 6 semanas
                d2 = df1[['store', 'prediction']].groupby( 'store' ).sum().reset_index()
                
                #mensagem a ser enviada
                msg = 'Store Number {} will sell R${:,.2f} in the next 6 weeks'.format(d2['store'].values[0],
                                                                                       d2['prediction'].values[0])
                
                #send message
                send_message( chat_id, msg )  
                return Response('OK', status=200)
                
            #send message
            else:
                send_message( chat_id, 'Loja não disponível.')  
                return Response('OK', status=200)
        else:
            send_message( chat_id, 'ID da loja está incorreto.')    
            #tem que colocar o status 200 se não a api não entende que acabou a msg
            return Response('OK', status=200)
                     
    else:
        return '<h1> Rossmann Telegram Bot </h1>'
    
    
if __name__ == '__main__':
    app.run( host ='0.0.0.0', port=5000 ) #5000 é a porta padrão do flask

#localhost.run é um serviço que faz um roteamento da maquiina local deixando ela disponivel na internet
#conecta a ssh na maquina e expoe ela ; na internet a porta padrão é 80, a da maquina é 5000 então vai fazer tipo um de para
#dessa forma, quando alguem mandar msg naquele end point da porta 80 redireciona para a maquina ; o telegram vai enviar para a porta 80 e o localhost rediciona para a 5000

#rodar isso no terminal para configurar a maquina:
#ssh -R 80:localhost:5000 ssh@localhost.run
#ssh -R 80:localhost:5000 ssh@localhost.run
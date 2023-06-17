import pickle
import inflection
import pandas as pd
import numpy as np
import math
import datetime

class Rossmann(object):
    def __init__(self):
        #caminho da pasta com os parametros
        self.home_path= ""
        
        #carregando os parametros
        self.competition_distance_scaler = pickle.load(open(self.home_path + 'parameter/competition_distance_scaler.pkl', 'rb'))
        self.promo_time_week_scaler = pickle.load(open(self.home_path + 'parameter/promo_time_week_scaler.pkl', 'rb'))
        self.competition_time_month_scaler = pickle.load(open(self.home_path + 'parameter/competition_time_month_scaler.pkl', 'rb'))
        self.year_scaler = pickle.load(open(self.home_path + 'parameter/year_scaler.pkl', 'rb'))
        self.store_type_scaler = pickle.load(open(self.home_path + 'parameter/store_type_scaler.pkl', 'rb'))

        
    def data_cleaning(self, df1):

        #renomeando as colunas
        cols_old = ['Store', 'DayOfWeek', 'Date', 'Open', 'Promo',
                   'StateHoliday', 'SchoolHoliday', 'StoreType', 'Assortment',
                   'CompetitionDistance', 'CompetitionOpenSinceMonth',
                   'CompetitionOpenSinceYear', 'Promo2', 'Promo2SinceWeek',
                   'Promo2SinceYear', 'PromoInterval']

        #função que deixa tudo minusculo e separado por _
        snakecase = lambda x: inflection.underscore(x)
        cols_new = list( map(snakecase, cols_old))

        df1.columns = cols_new

        #mudando o tipo da data
        df1['date'] = pd.to_datetime(df1['date'])

        #preenchendo os NA

        #CompetitionDistance - distance in meters to the nearest competitor store
        #vou assumir que se o valor é mt maior que a distância máxima que tem um competidor proximo então é a msm coisa que dizer que não tem um competidor proxim

        max_value = df1['competition_distance'].max()
        #isnan avalia se é NA
        df1['competition_distance'] = df1['competition_distance'].apply( lambda x: 200000.0 if math.isnan(x) else x)

        #CompetitionOpenSince[Month/Year] - gives the approximate year and month of the time the nearest competitor was opened
        #vou assumir que é vazio pqe não tem um competidor mais proximo ou tem um competidor proximo mas não temos info de quando abriu
        #vou substituir pelo mês da data da venda

        df1['competition_open_since_month'] = df1.apply( lambda x: x['date'].month if math.isnan(x['competition_open_since_month']) else x['competition_open_since_month'], axis=1)

        df1['competition_open_since_year'] = df1.apply( lambda x: x['date'].year if math.isnan(x['competition_open_since_year']) else x['competition_open_since_year'], axis=1)

        #Promo2Since[Year/Week] - describes the year and calendar week when the store started participating in Promo2
        #vou substituir pela data da venda

        df1['promo2_since_week'] = df1.apply( lambda x: x['date'].week if math.isnan(x['promo2_since_week']) else x['promo2_since_week'], axis=1)

        df1['promo2_since_year'] = df1.apply( lambda x: x['date'].year if math.isnan(x['promo2_since_year']) else x['promo2_since_year'], axis=1)

        #PromoInterval - describes the consecutive intervals Promo2 is started, naming the months the promotion is started anew. E.g. "Feb,May,Aug,Nov" means each round starts in February, May, August, November of any given year for that store
        #dict para trocar o num pelo nome do mês
        month_map = {1: 'Jan',
                     2: 'Fev',
                     3: 'Mar',
                     4: 'Apr',
                     5: 'May',
                     6: 'Jun',
                     7: 'Jul',
                     8: 'Aug',
                     9: 'Sept',
                     10: 'Oct',
                     11: 'Nov',
                     12: 'Dec'}

        #susbtitui o N/A por zero
        df1['promo_interval'].fillna(0, inplace=True)

        #extraindo o mês da data e aplicando o dicionario para fazer a tradução
        df1['month_map'] = df1['date'].dt.month.map(month_map)

        #avaliação se o month_map está dentro do intervalo para ver se a loja está na promoção (1) ou não (0)
        df1['is_promo'] = df1[['promo_interval','month_map']].apply( lambda x: 0 if x['promo_interval'] == 0 else 1 if x['month_map'] in x['promo_interval'].split(',') else 0, axis=1)

        #mudando os tipos de dados
        #passando para inteiro
        df1['competition_open_since_month'] = df1['competition_open_since_month'].astype(int)

        df1['competition_open_since_year'] = df1['competition_open_since_year'].astype(int)

        df1['promo2_since_week'] = df1['promo2_since_week'].astype(int)

        df1['promo2_since_year'] = df1['promo2_since_year'].astype(int)
        
        return df1
    
    def feature_engineering(self, df2):
    
        #year
        df2['year'] = df2['date'].dt.year

        #month
        df2['month'] = df2['date'].dt.month

        #day
        df2['day'] = df2['date'].dt.day

        #week of year
        df2['week_of_year'] = df2['date'].dt.isocalendar().week

        #year week
        df2['year_week'] = df2['date'].dt.strftime('%Y-%W')

        #cometition since
        #juntando as duas informações de data (ano e mês) em uma coluna só
        #datetime.datetime() é uma função que monta uma data apartir de valores
        df2['competition_since'] = df2.apply( lambda x: datetime.datetime(year = x['competition_open_since_year'], month = x['competition_open_since_month'], day=1), axis=1)
        df2['competition_time_month'] = ((df2['date'] - df2['competition_since'])/30 ).apply(lambda x: x.days).astype(int)

        #promo since
        df2['promo_since'] = df2['promo2_since_year'].astype(str) + '-' + df2['promo2_since_week'].astype(str)

        #trocando caracter para o tipo data
        df2['promo_since'] = df2['promo_since'].apply(lambda x: datetime.datetime.strptime( x + '-1', '%Y-%W-%w') - datetime.timedelta(days=7))

        df2['promo_time_week'] = ((df2['date'] - df2['promo_since'])/7).apply(lambda x: x.days).astype(int)

        #assortment
        #Assortment - describes an assortment level: a = basic, b = extra, c = extended
        df2['assortment'] = df2['assortment'].apply(lambda x: 'basic' if x == 'a' else 'extra' if x == 'b' else 'extended')

        #state holiday
        #StateHoliday - indicates a state holiday. Normally all stores, with few exceptions, are closed on state holidays. Note that all schools are closed on public holidays and weekends. 
        #a = public holiday, b = Easter holiday, c = Christmas, 0 = None

        df2['state_holiday'] = df2['state_holiday'].apply( lambda x: 'public_holiday' if x == 'a' else 'easter_holiday' if x == 'b' else 'christmas' if x == 'c' else 'regular_day')

        ######filtragem de variáveis
        #filtrando linhas
        #só vou usar as vendas em que open é diferente de zero
        #sellers tem que ser maior que zero
        df2 = df2[df2['open'] != 0]
        
        #seleção das colunas 
        #baseado no contexto, não vamos ter a coluna customers no momento da previsão pqe não tem como prever quantos customers vai ter
        #promo interval foi derivada e month map é uma coluna auxiliar
        #a coluna open como foi filtrada só tem valor 1
        cols_drop = ['open','promo_interval','month_map']
        df2 = df2.drop(cols_drop, axis=1)
        
        return df2
    
    def data_preparation(self, df5):
        ## 5.2 Rescaling
        #Técnica Robust Scaler
        #criando a msm variavel mas em uma nova escala

        #competition_distance
        df5['competition_distance'] = self.competition_distance_scaler.fit_transform(df5[['competition_distance']].values)

        #competition time month
        df5['competition_time_month'] = self.competition_time_month_scaler.fit_transform(df5[['competition_time_month']].values)

        #Técnica Min-Max Scaler:
        #promo time week
        df5['promo_time_week'] = self.promo_time_week_scaler.fit_transform(df5[['promo_time_week']].values)

        #year
        df5['year'] = self.year_scaler.fit_transform(df5[['year']].values)

        ## 5.3 Transformação
        ### 5.3.1 Encoding
        #variaveis categoricas: state_holiday , store_type e assortment

        #state_holiday - One Hot Encoding
        #cria uma coluna pra cada tipo colocando 1 ou 0
        df5 = pd.get_dummies( df5, prefix=['state_holiday'], columns = ['state_holiday'])

        #store_type - Label Encoding
        #substitui a categoria por número
        df5['store_type'] = self.store_type_scaler.fit_transform(df5['store_type'])

        #assortment - Ordinal Encoding
        #substitui a categoria por número de forma ordenada
        assortment_dict = {'basic': 1,
                           'extra': 2,
                           'extended': 3}
        df5['assortment'] = df5['assortment'].map(assortment_dict)

        ### 5.3.3 Nature Transformation

        #separando quais variáveis tem natureza ciclica 
        #month
        df5['month_sin'] = df5['month'].apply( lambda x: np.sin( x * (2. * np.pi/12)))
        df5['month_cos'] = df5['month'].apply( lambda x: np.cos( x * (2. * np.pi/12)))

        #day
        df5['day_sin'] = df5['day'].apply( lambda x: np.sin( x * (2. * np.pi/30)))
        df5['day_cos'] = df5['day'].apply( lambda x: np.cos( x * (2. * np.pi/30)))

        #week of year
        df5['week_of_year_sin'] = df5['week_of_year'].apply( lambda x: np.sin( x * (2. * np.pi/52)))
        df5['week_of_year_cos'] = df5['week_of_year'].apply( lambda x: np.cos( x * (2. * np.pi/52)))

        #day_of_week
        df5['day_of_week_sin'] = df5['day_of_week'].apply( lambda x: np.sin( x * (2. * np.pi/7)))
        df5['day_of_week_cos'] = df5['day_of_week'].apply( lambda x: np.cos( x * (2. * np.pi/7)))

        cols_selected = ['store', 'promo', 'store_type', 'assortment', 'competition_distance', 'competition_open_since_month', 'competition_open_since_year', 'promo2', 'promo2_since_week', 'promo2_since_year', 'competition_time_month', 'promo_time_week', 'month_cos', 'month_sin', 'day_sin', 'day_cos', 'week_of_year_sin', 'week_of_year_cos', 'day_of_week_sin', 'day_of_week_cos']
        
        return df5[cols_selected]
    
    def get_prediction(self, model, original_data, test_data):
        #prediction
        pred = model.predict(test_data)
        
        #join prediction into the original data
        #original_data['prediction'].np.expm1(pred)
        
        original_data['prediction'] = np.expm1(pred)
        
        return original_data.to_json(orient='records')

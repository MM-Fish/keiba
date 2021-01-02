# 学習準備
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import KFold
from sklearn.model_selection import train_test_split
from CommonFunction import CommonFunction

class PreLearning(CommonFunction):
    def __init__(self, df):
        self.df = df.copy()
        for i in list(self.df.select_dtypes('<M8[ns]').columns):
            self.df[i] = self.df[i].map(str)

    def rank_coef(self, binomial=True, rank=3):
        self.df['RankCoef'] = 0
        rank1 = rank + 1
        if binomial==True:
            self.df.loc[self.df['Rank'] < rank1, 'RankCoef'] = 1
        if binomial==False:
            self.df.loc[self.df['Rank'] < rank1, 'RankCoef'] = self.df.loc[self.df['Rank'] < rank1, 'Rank']

    def divide_train_test(self, year):
        self.train = self.df.loc[ self.df['Year'] <= year, ].copy()
        self.test = self.df.loc[ self.df['Year'] > year, ].copy()
        self.train.reset_index(inplace=True, drop=True)
        self.test.reset_index(inplace=True, drop=True)
        # print('self.train', len(self.train['RaceID'].unique()), 'self.test', len(self.test['RaceID'].unique()))
        # print('train最新', self.train['Date'].max(), 'test最古',self.test['Date'].min())

    def divide_x_y(self, drop_col):
        self.xtrain = self.train.drop(drop_col, axis=1)
        self.ytrain = self.train['RankCoef']

        self.xtest = self.test.drop(drop_col, axis=1)
        self.ytest = self.test['RankCoef']

        self.categorical = list(self.xtest.select_dtypes(object).columns)
  
    # 学習データより不足している特徴量をNaNで埋める
    def x_by_today_race(self, today_x):
        self.today_data = today_x.copy()
        for i in list(self.today_data.select_dtypes('<M8[ns]').columns):
            self.today_data[i] = self.today_data[i].map(str)
        print([ i for i in self.xtrain.columns if i not in self.today_data ])
        today_x1 = pd.concat([self.today_data, self.xtrain]).iloc[range(0, len(self.today_data)), ][self.xtrain.columns]
        self.today_x = today_x1.copy()

    def label_encoding(self, today=False):
      for c in self.categorical:
          le = LabelEncoder()
          le.fit(self.df[c].fillna('NA'))
          
          if today==False:
              self.xtrain[c] = le.transform(self.xtrain[c].fillna('NA'))
              self.xtest[c] = le.transform(self.xtest[c].fillna('NA'))
          if today==True:
              self.today_x[c] = le.transform(self.today_x[c].fillna('NA'))

    # trainを更にtrainとvalidに分ける
    def divide_train_valid(self, train_size=0.8):
        tr_raceid, va_raceid = train_test_split(self.train['RaceID'].drop_duplicates(), train_size=train_size)
        self.tr_x, self.va_x = self.xtrain.loc[self.train['RaceID'].isin(tr_raceid), ], self.xtrain.loc[self.train['RaceID'].isin(va_raceid), ]
        self.tr_y, self.va_y = self.ytrain.loc[self.train['RaceID'].isin(tr_raceid), ], self.ytrain.loc[self.train['RaceID'].isin(va_raceid), ]
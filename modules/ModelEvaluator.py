# モデル精度評価
import math
import pandas as pd
import numpy as np
from scipy.special import comb
from CommonFunction import CommonFunction
from tqdm.notebook import tqdm
from sklearn.metrics import roc_auc_score

class ModelEvaluator(CommonFunction):
    def __init__(self, model, xtest, test):
        self.model = model
        self.xtest = xtest
        self.test = test
    
    # ３着以内に入る確率
    def predict_proba(self, std=True):
        proba = self.test.loc[:, ['RaceID', 'ID', 'Odds', 'No_']]
        proba['proba'] = self.model.predict_proba(self.xtest)[:, 1]
        if std:
            standard_scaler = lambda x: (x - x.mean()) / x.std() # 標準化
            proba['proba'] = proba.groupby('RaceID')['proba'].transform(standard_scaler)
            proba['proba'] = (proba['proba'] - proba['proba'].min()) / (proba['proba'].max() - proba['proba'].min()) # min-maxスケーリング
        return proba
    
    # 予測順位
    def pred_rank(self, with_rank=True):
        proba_table = self.predict_proba()
        RaceIDs = proba_table['RaceID'].unique()
        for i in RaceIDs:
            proba_table.loc[(proba_table['RaceID'] == i), 'PRank'] = proba_table.loc[(proba_table['RaceID'] == i), 'proba'].rank(ascending=False).values # probaの高い順から順位をつける
        if with_rank==True:
            rank = self.test[['ID', 'Rank']]
            proba_table= pd.merge(proba_table, rank, on='ID', how="left")
        return proba_table

    # 的中率
    def hit_ratio(self, rank, above=True):
        proba_table = self.pred_rank()
        if above==True:
            hit_ratio = len(proba_table.loc[(proba_table['PRank']<=rank) & (proba_table['Rank']<=rank), ])\
            / len(proba_table.loc[proba_table['Rank']<=rank, ])
        else:
            hit_ratio = len(proba_table.loc[(proba_table['PRank']==rank) & (proba_table['Rank']==rank), ])\
            / len(proba_table.loc[proba_table['Rank']==rank, ])
        return hit_ratio
            

    # 閾値設定
    def predict(self, threshold=0.5):
        y_pred = self.predict_proba()['proba']
        # thresholdを超えたら1, 超えなければ0
        return [0 if p<threshold else 1 for p in y_pred]

    #AUCスコアを出力
    def score(self, y_true):
        return roc_auc_score(y_true, self.predict_proba()['proba'])

    #変数の重要度を出力。n_displayで上から何個出力するかを指定。
    def feature_importance(self, n_display=20):
        importances = pd.DataFrame({"features": self.xtest.columns, 
                                    "importance": self.model.feature_importances_})
        return importances.sort_values("importance", ascending=False)[:n_display]

    def pred_table(self, threshold=0.5, bet_only=True):
        pred_table = self.test.loc[:, ['RaceID', 'ID', 'Odds', 'No_']]
        pred_table['pred'] = self.predict(threshold)
        if bet_only:
            return pred_table[pred_table['pred']==1]
        else:
            return pred_table
    
    def merge_return_data(self, return_data, threshold=0.5):
        pred_table = self.pred_table(threshold)
        df = return_data.copy()
        df = pred_table.merge(df, left_on='RaceID', right_index=True, how='left')
        return df    
    
    # tansho
    def tansho_return(self, tansho, box=0, just=False, proper=False, bet_type=None, threshold=0.5):
        df = self.merge_return_data(tansho, threshold)
        n_bets = len(df)
        n_bet_races = len(df['RaceID'].unique())
        money = 0
        for i in range(10):
            try:
                df.loc[ df['No_']==df[f'win_{i}'], 'win' ] = 1
                money += df[df[f'win_{i}']==df['No_']][f'return_{i}'].sum()
            except KeyError:
                break
        df1 = df.loc[df['win']==1, ]
        
        if proper==False:
            return_rate = money / (n_bets*100)
        else:
            # 常に回収率が一定になるように賭ける場合の回収率
            df['Odds1'] = df['Odds'].map(lambda x: 1/x)
            return_rate = len(df.query('win_0 == No_')) / df['Odds1'].sum()
        return n_bet_races, n_bets, return_rate, df1
    
    # fukusho
    def fukusho_return(self, fukusho, box=0, just=False, proper=False, bet_type=None, threshold=0.5):
        df = self.merge_return_data(fukusho, threshold)
        n_bets = len(df)
        n_bet_races = len(df['RaceID'].unique())
        money = 0
        for i in range(10):
            try:
                df.loc[ df['No_']==df[f'win_{i}'], 'win' ] = 1
                money += df[df[f'win_{i}']==df['No_']][f'return_{i}'].sum()
            except KeyError:
                break
        df1 = df.loc[df['win']==1, ]

        return_rate = money / (n_bets*100)
        return n_bet_races, n_bets, return_rate, df1
    
    # umaren, wide, sanrenfuku
    def renfuku_return(self, return_table, box, just=False, proper=False, bet_type=None, threshold=0.5):
        df = self.merge_return_data(return_table, threshold)
        df2 = pd.DataFrame()
        money = 0
        if bet_type == 'umaren':
            x = 2
        elif bet_type == 'wide':
            x = 2
        elif bet_type == 'sanrenfuku':
            x = 3
        else:
            raise Exception('bet_typeが異なります')
        
        if box < x:
            raise Exception('box must be >= x')

        for i in range(10):
            try:
                grouped = df.groupby(['RaceID'])['No_'].count()
                if (just==True):
                    n_bets_dict = grouped.loc[grouped==box].to_dict()
                if (just==False):
                    n_bets_dict = grouped.loc[grouped>=box].to_dict()
                n_bets = sum( [comb(i, x) for i in n_bets_dict.values()] )
                n_bet_races = len(n_bets_dict)
                df = df.loc[df['RaceID'].isin(n_bets_dict.keys()), ]

                # x着以内に入る場合は，wins_0を，1と印付ける
                for j in range(x):
                      df.loc[ df['No_']==df[f'win_{i}{j}'], f'win_{i}' ] = 1
                df[f'win_{i}'] = df[f'win_{i}'].fillna(0)

                # wins_0の合計がxのレースIDを取得
                grouped = df.groupby(['RaceID'])[f'win_{i}'].sum()
                wins_raceid_list = grouped.loc[grouped==x].index

                # 払い戻し金計算
                df1 = df.loc[df['RaceID'].isin(wins_raceid_list), ]
                df2 = pd.concat([df2, df1])
                money += df1.drop_duplicates(['RaceID'])[f'return_{i}'].sum()
            except KeyError:
                break

        if n_bets != 0:
            return_rate = money / (n_bets*100)
        else:
            return_rate = 0
        return n_bet_races, n_bets, return_rate, df2

    # umatan, sanrentan
    def rentan_return(self, return_table, box, just=False, proper=False, bet_type=None, threshold=0.5):
        df = self.merge_return_data(return_table, threshold)
        df2 = pd.DataFrame()
        money = 0

        if bet_type == 'umatan':
            x = 2
        elif bet_type == 'sanrentan':
            x = 3
        else:
            raise Exception('bet_typeが異なります')

        if box < x:
            raise Exception('box must be >= x')
        
        for i in range(10):
            try:
                grouped = df.groupby(['RaceID'])['No_'].count()
                if (just==True):
                    n_bets_dict = grouped.loc[grouped==box].to_dict()
                if (just==False):
                    n_bets_dict = grouped.loc[grouped>=box].to_dict()
                n_bets = sum( [comb(i, x) * math.factorial(x) for i in n_bets_dict.values()] )
                n_bet_races = len(n_bets_dict)
                df = df.loc[df['RaceID'].isin(n_bets_dict.keys()), ]

                # x着以内に入る場合は，wins_0を，1と印付ける
                for j in range(x):
                      df.loc[ df['No_']==df[f'win_{i}{j}'], f'win_{i}' ] = 1
                df[f'win_{i}'] = df[f'win_{i}'].fillna(0)

                # wins_0の合計がxのレースIDを取得
                grouped = df.groupby(['RaceID'])[f'win_{i}'].sum()
                wins_raceid_list = grouped.loc[grouped==x].index

                # 払い戻し金計算
                df1 = df.loc[df['RaceID'].isin(wins_raceid_list), ]
                df2 = pd.concat([df2, df1])
                money += df1.drop_duplicates(['RaceID'])[f'return_{i}'].sum()
            except KeyError:
                break
        return_rate = money / (n_bets*100)
        return n_bet_races, n_bets, return_rate, df2

    # 閾値別回収率計算
    def gain(self, return_table, box=0, just=False, proper=False, bet_type=None, n_samples=100, lower=100, min_threshold=0.5):
        if bet_type=='fukusho':
            return_func = self.fukusho_return
        elif bet_type=='tansho':
            return_func = self.tansho_return
        elif (bet_type=='umaren') | (bet_type=='wide') | (bet_type=='sanrenfuku'):
            return_func = self.renfuku_return
        elif (bet_type=='umatan') | (bet_type=='sanrentan'):
            return_func = self.rentan_return
        else:
            raise Exception('bet_typeが異なります')

        gain1 = {}
        gain2 = {}
        for i in tqdm(range(n_samples)):
            threshold = 1 * i / n_samples + min_threshold * (1-(i/n_samples))
            n_bet_races, n_bets, return_rate, df1 = return_func(return_table, box, just, proper, bet_type, threshold)
            if n_bet_races > lower:
                gain1[n_bet_races] = return_rate
                gain2[threshold] = return_rate
            else:
                break
        return pd.Series(gain1), pd.Series(gain2)
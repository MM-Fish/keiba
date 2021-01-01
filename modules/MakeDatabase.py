import csv
import pandas as pd
import numpy as np
import datetime
import time
import re
from tqdm import tqdm
import os
from CommonFunction import CommonFunction

# 親クラス作成
class MakeDatabase(CommonFunction):
    def __init__(self, race=False, past_race=False, horse_info=False, peds=False, train=False, colab=True):
        self.race = race
        self.past_race = past_race
        self.horse_info = horse_info
        self.peds = peds
        self.train = train
        if self.race==True:
            self.race_df = pd.DataFrame()
        if self.past_race==True:
            self.past_race_df = pd.DataFrame()
        if self.horse_info==True:
            self.horse_info_df = pd.DataFrame()
        if self.peds==True:
            self.peds_df = pd.DataFrame()
        if self.train==True:
            self.train_df = pd.DataFrame()
        self.colab = colab
        self.loc_dict = {'札幌': '01', '函館': '02', '福島': '03', '新潟': '04', '東京': '05', '中山': '06', '中京': '07', '京都': '08', '阪神': '09', '小倉': '10'}
    

    # 出馬表，過去レースデータ，当日出馬表に適用
    def edit_race_df(self, df):
        df1 = df.copy()
        df1 = df1.astype(str)
        to_float_list = []
        drop_list = []

        # Rank
        if 'Rank' in df1.columns:
            df1.loc[df1['Rank'].astype(str).str.contains(r"\D"), 'Rank'] = 100
            print('Rank', df1['Rank'].unique())
            to_float_list.append('Rank')

        # Sex_Age
        if 'Sex_Age' in df1.columns:
            df1.loc[:, 'Sex'] = df1['Sex_Age'].map(lambda x: str(x)[0])
            df1.loc[:, 'Age'] = df1['Sex_Age'].map(lambda x: str(x)[1])
            to_float_list.append('Age')
            drop_list.append('Sex_Age')
            print('Sex', df1['Sex'].unique())
            print('Age', df1['Age'].unique())

        # Odds
        if 'Odds' in df1.columns:
            df1.loc[~df1['Odds'].astype(str).str.contains(r"\d"), 'Odds'] = np.nan
            to_float_list.append('Odds')

        # Weight_DifWeight
        if 'Weight_DifWeight' in df1.columns:
            df1.loc[~df1['Weight_DifWeight'].astype(str).str.contains(r"\d"), 'Weight_DifWeight'] = np.nan
            df1.loc[~df1['Weight_DifWeight'].isnull(), 'Weight'] = df1.loc[~df1['Weight_DifWeight'].isnull(), 'Weight_DifWeight'].str.split("(", expand=True)[0]
            df1.loc[~df1['Weight_DifWeight'].isnull(), 'DifWeight'] = df1.loc[~df1['Weight_DifWeight'].isnull(), 'Weight_DifWeight'].str.split("(", expand=True)[1].str[:-1]
            to_float_list += ['Weight', 'DifWeight']
            drop_list.append('Weight_DifWeight')
          
        # Prize
        if 'Prize' in df1.columns:
            df1.loc[df['Prize'].isnull(),'Prize'] = 0
            to_float_list.append('Prize')

        # Racetype, Length
        if 'Racetype_Length' in df1.columns:
            df1['Racetype'] = df1['Racetype_Length'].map(lambda x: re.sub(r"\d", "", x))
            df1['Racetype'] = df1['Racetype'].map(lambda x: x.replace(r"m", '').strip())
            df1['Length'] = df1['Racetype_Length'].map(lambda x: re.sub(r"\D", "", x))
            df1.loc[df1['Length'] == '', 'Length'] = np.nan
            to_float_list.append('Length')
            drop_list.append('Racetype_Length')
            print(df1['Racetype'].unique())
            print(df1['Length'].unique())
        
        # R (RaceIDを作成するために，ここでfloatに変換する)
        if 'R' in df1.columns:
            df1['R'] = df1['R'].astype(float)
        
        # No_ (IDを作成するために，ここでfloatに変換する)
        if 'No_' in df1.columns:
            df1['No_'] = df1['No_'].astype(float)
        
        # Year
        if 'Date' in df1.columns:
            df1['Year'] = df1['Date'].map(lambda x: x[0:4])
            to_float_list.append('Year')
            print('Year', df1['Year'].unique())
        
        # Term, Day(ある場合)
        if ('Term' in df1.columns) & ('Day' in df1.columns):
            to_float_list += ['Term', 'Day']
        
        # Location, Term, Day(ない場合)
        if 'Kaisai' in df1.columns:
            df1['Location'] = df1['Kaisai'].map(lambda x: self.for_kaisai(x)[1])
            df1['Term'] = df1['Kaisai'].map(lambda x: self.for_kaisai(x)[0])
            df1['Day'] = df1['Kaisai'].map(lambda x: self.for_kaisai(x)[2])
            to_float_list += ['Term', 'Day']
            drop_list.append('Kaisai')
            print('Location', df1['Location'].unique())
            print('Term', df1['Term'].unique())
            print('Day', df1['Day'].unique())

        # Weather，Condition
        if 'Weather' in df1.columns:
            print(df1['Weather'].unique())
        if 'Condition' in df1.columns:
            print(df1['Condition'].unique())
        
        # ID作成
        df1['ID'] = df1['RaceID'] + df1['No_'].map(lambda x:str(x)[:-2].zfill(2))

        # Time
        if 'Time' in df1.columns:
            df1['Time'] = df1['Time'].map(self.for_time)
            to_float_list.append('Time')

        # Tuuka
        if 'Tuuka' in df1.columns:
            tuuka_df = df1['Tuuka'].str.split("-", expand=True)
            tuuka_df.columns = [ f"{i}C" for i in range(1,5)]
            df1.drop(["Tuuka"], axis=1, inplace=True)
            df1 = pd.concat([df1, tuuka_df], axis=1)

        # ID類以外の数字をfloatに変換
        for float_col in ['Number', 'Waku', 'No_', 'Ninki', 'Kinryou', 'CondCoef', 'SpdCoef', 'G3F']:
            if float_col in df1.columns:
                to_float_list.append(float_col)
        for float_col in to_float_list:
            df1[float_col] = df1[float_col].astype(float)

        # 不要な列削除
        for drop_col in ['Tyoukyou', 'Comment', 'Eizou', 'Shirushi', 'Touroku', 'Memo']:
            if drop_col in df1.columns:
                drop_list.append(drop_col)
        # print(drop_list)
        df1 = df1.drop(drop_list, axis=1)
        
        # 重複行削除
        df1 = df1.drop_duplicates()

        return df1


    # 競走馬基本情報編集
    def edit_horse_info(self, df):
        df1 = df.copy()
        df1 = df1.reindex(columns=['HorseID', 'Name', 'Birth', 'Trainer', 'Owner', 'Bosyuu', 'Breeder', 'Santi', 'Seri', 'TotalPrize', 'Seiseki', 'Katikura', 'Relatives', 'TrainerID', 'OwnerID', 'BreederID'])
        indexes = df1.loc[df1['Bosyuu'] == '生産者', ].index
        indexes = list(indexes) + list(indexes + 1)
        indexes.sort()
        df1_revised = pd.concat([df1.iloc[indexes, : 5 ], df1.iloc[indexes, 5 : 13 ].shift(1, axis=1), df1.iloc[indexes, 13 : ]], axis=1)
        df1.iloc[indexes, : ] = df1_revised

        for i in range(2, len(df1.columns)-4):
            print(df1.iloc[range(0, len(df1), 2), i].unique())
        
        # 奇数番号のみ取得
        df1 = df1.iloc[range(1, len(df1), 2), ]

        # 重複行削除
        df1 = df1.drop_duplicates(subset='HorseID')

        return df1
    

    # 調教データ編集
    def edit_train(self, df):
        df1 = df.copy()
        df1['Date'] = df1['Date'].map(lambda x: re.sub(r'\(.\)', '', x ))
        df1['Position'] = df1['Position'].map( lambda x : self.to_float(x) )

        df2 = df1['TimeAll'].str.split('li', expand=True)
        col_list = ['6F', '5F', '4F', '3F', '1F']
        df2.columns = col_list + ['Comment']

        for col in col_list:
            df2[col] = df2[col].map( lambda x : self.for_train_time(x) )
            df2[col] = df2[col].map( lambda x : self.to_float(x) )
        
        df2['Lap'] = 'F'
        df2.loc[df2['3F'] < 20, 'Lap'] = 'T'
        
        return pd.concat([df1, df2], axis=1).drop(['TimeAll', 'Eizou'], axis=1)


    # データ紐付け
    def conect_data(self, today=False):
        # horse_infoをもとに，past_raceに競走馬名追加
        if (self.past_race==True) & (self.horse_info==True):
            self.conect(self.horse_info_df, self.past_race_df, 'HorseID', 'Name')

        # past_raceをもとに，raceにレース情報追加
        if (self.race==True) & (self.past_race==True) & (today==False):
            add_col_list = ['Date', 'Year', 'Number', 'Location', 'Term', 'Day', 'R', 'Racetype', 'Length', 'Condition', 'CondCoef', 'Pace']
            for add_col in add_col_list:
                self.conect(self.past_race_df, self.race_df, 'RaceID', add_col)


    # pickleファイルに保存
    def save_pickle(self, dir_name, today=False):
        if self.colab==True:
            dir_name = f'drive/My Drive/keiba/{dir_name}'
        if self.race==True:
            if today==True:
                self.race_df.to_pickle(f'{dir_name}/today_race_all.csv')
            if today==False:
                self.race_df.to_pickle(f'{dir_name}/shutuba_all.csv')
        if self.past_race==True:
            self.past_race_df.to_pickle(f'{dir_name}/past_race_all.csv')
        if self.horse_info==True:
            self.horse_info_df.to_pickle(f'{dir_name}/horse_info_all.csv')
        if self.peds==True:
            self.peds_df.to_pickle(f'{dir_name}/peds_all.csv')
        if self.train==True:
            self.train_df.to_pickle(f'{dir_name}/train_all.csv')


    # データ更新
    def update_data(self, race_all, past_race_all, horse_info_all, peds_all):
      self.past_race = pd.concat([self.past_race, past_race_all])
      self.horse_info = pd.concat([self.horse_info, horse_info_all])


    # 必要関数定義
    def conect(self, from_df, to_df, axis_col, add_col):
        conect_values = from_df.loc[~from_df[axis_col].isnull(), [axis_col, add_col]].values
        # 辞書作成
        conect_dict = {}
        for axis_value, add_value in conect_values:
          conect_dict[axis_value] = add_value

        # 追加
        to_df[add_col] = to_df[axis_col].map(lambda x: self.change_value_by_dict(x, conect_dict, True))

    def for_time(self, x):
        try:
            x = x.replace(':','.')
            x = pd.to_datetime(x, format='%M.%S.%f') - pd.to_datetime('19000101')
            x = x.total_seconds()
            # print(x)
            return x
        except AttributeError:
            # print('for_time: AttributeError', x)
            return x
        except ValueError:
            print('for_time: ValueError', x)
            return np.nan

    def for_kaisai(self, x):
        try:
            if x[0].isdecimal():
                x1 = re.sub(r"\d", "", x[1:]) #文字のみ取得
                x2 = re.sub(r"\D", "", x[1:]) #数字のみ取得
                return x[0].zfill(2), x1, x2.zfill(2)
            else:
                return np.nan, x, np.nan
        except TypeError:
            print('for_kaisai: TypeError', x)
            return np.nan, x, np.nan
    
    def for_train_time(self, x):
        time = re.search(r"[0-9]{2}\.[0-9]", x)
        if time != None:
          return time.group()
        else:
          return None


# 払い戻しデータ加工
class Return:    
    def __init__(self, return_tables):
        self.return_tables = return_tables.drop_duplicates().set_index('RaceID')
    
    @property
    def tansho(self):
        tansho = self.tansho_fukusho('単勝')
        return tansho
    
    @property
    def fukusho(self):
        fukusho = self.tansho_fukusho('複勝')
        return fukusho
    
    @property
    def wakuren(self):
        wakuren = self.others('枠連')
        return wakuren
    
    @property
    def umaren(self):
        umaren = self.others('馬連')
        return umaren

    @property
    def wide(self):
        wide = self.others('ワイド')
        return wide
    
    @property
    def umatan(self):
        umatan = self.others('馬単')
        return umatan
    
    @property
    def sanrenfuku(self):
        sanrenfuku = self.others('三連複')
        return sanrenfuku
    
    @property
    def sanrentan(self):
        sanrentan = self.others('三連単')
        return sanrentan
    
    # 単勝，複勝
    def tansho_fukusho(self, bet_type):
        df = self.return_tables[self.return_tables[0]==bet_type][[1,2]]

        #文字'br'で区切る
        wins = df[1].str.split('br', expand=True).add_prefix('win_')
        returns = df[2].str.split('br', expand=True).add_prefix('return_')
        df1 = pd.concat([wins, returns], axis=1)
        
        for column in df1.columns:
            df1[column] = df1[column].str.replace(',', '')
        return df1.fillna(0).astype(float)
    
    # 他の賭け方
    def others(self, bet_type):
        df = self.return_tables[self.return_tables[0]==bet_type][[1,2]]

        #文字'br'で区切る
        wins = df[1].str.split('br', expand=True).add_prefix('win_')
        returns = df[2].str.split('br', expand=True).add_prefix('return_')
        df1 = pd.concat([wins, returns], axis=1)
        
        #文字'→'or'-'で区切る
        if bet_type in ['馬単', '三連単']:
            key_split = '→'
        elif bet_type in ['枠連', '馬連', 'ワイド', '三連複']:
            key_split = '-'
        else:
            raise Exception("bet_typeが正しくありません。")

        df_col_list = list(df1.columns)
        win_list = [i for i in df_col_list if 'win' in i]
        return_list = [i for i in df_col_list if 'return' in i]

        df3 = pd.DataFrame(df1.index)
        for win_col in win_list:
          df2 = df1[win_col].str.split(key_split, expand=True).add_prefix(win_col)
          df3 = df3.merge(df2, left_on = 'RaceID', right_index = True)
        df2 = df3.merge( df1[return_list], left_on = 'RaceID', right_index = True )
        
        df2 = df2.set_index('RaceID')

        for column in df2.columns:
            df2[column] = df2[column].str.replace(',', '')
        
        return df2.fillna(0).astype(float)
# データ加工
import collections
import pandas as pd
import numpy as np
from CommonFunction import CommonFunction

class DataProcessor(CommonFunction):
    def __init__(self, shutuba, past_race, horse_info, peds, train):
        self.shutuba = shutuba
        self.past_race = past_race
        self.horse_info = horse_info
        self.peds = peds
        self.train = train
        
        # 日付のデータ型変更
        self.shutuba.loc[:, 'Date'] = pd.to_datetime(self.shutuba['Date'], format='%Y/%m/%d')
        self.past_race.loc[:, 'Date'] = pd.to_datetime(self.past_race['Date'], format='%Y/%m/%d')
        self.train.loc[:, 'Date'] = pd.to_datetime(self.train['Date'], format='%Y/%m/%d')
        
        # 重複削除
        self.shutuba = self.shutuba.drop_duplicates(['HorseID', 'Date'])
        self.past_race = self.past_race.drop_duplicates(['HorseID', 'Date'])
        
    # 2次元numpy配列を作成(1, 2, 3全てに使用)
    # 各行にデータフレームの列の値を格納
    # 例）[[HorseIDの値],[Conditionの値],...]
    def pd_to_np(self, df, target, col_list, id_list=[]):
        if len(id_list) != 0:
            df1 = df.loc[df[target].isin(id_list), ]
        else:
            df1 = df.copy()
        col_list1 = [target, 'Date'] + col_list
        i = 0  
        for colname in col_list1:
            if i == 0:
                all_col_values = df1[ colname ].values
                all_col_values1 = all_col_values.reshape(1, len(all_col_values))
            else:
                col_values = df1[ colname ].values
                col_values1 = col_values.reshape(1, len(col_values))
                all_col_values1 = np.concatenate([all_col_values1, col_values1])
            i += 1
        return all_col_values1


    # 1. past_raceを集約して結合(numpy)
    # 過去n_race分の平均データを横に結合した２次元numpy配列を作成
    # 始めの２列はtargetと'Date'
    # 例）[[target, Date, Rank, SpdCoef,..],
    #      [target, Date, Rank, SpdCoef,..],..]
    def npdata_av(self, result_col_list, n_race, all_col_values):
        horse_data_counts = collections.Counter(all_col_values[0])
        nrow = len(all_col_values[0])
        ncol = len(result_col_list) + 2
        npdata = np.full((nrow, ncol),np.nan)
        date = all_col_values[1]
        m = 0
        for k, v in horse_data_counts.items():
            for j in range(v):
                npdata[m,0] = k
                npdata[m,1] = date[m]
                n_race1 = v - (j + 1)
                if n_race1 >= n_race:
                    n_race1 = n_race
                past_npdata = all_col_values[ 2:, m : m+n_race1+1 ]
                if n_race1 > 0:
                    try:
                        results_av = np.nanmean(past_npdata[:, 1:], axis=1)
                        npdata[m, 2 : ncol] = results_av
                        m += 1
                    except ZeroDivisionError:
                        m += 1
                        continue
                else: 
                    m += 1
        return npdata
    
    def merge_av_all(self, target, result_col_list, n_race=100, today=False):
        results_error_col_list = [result_col for result_col in result_col_list if self.past_race[result_col].dtypes != np.dtype(float)]
        if len(results_error_col_list) > 0:
            raise TypeError(results_error_col_list, "result_col_listにfloat以外のデータ型が含まれています")

        self.past_race = self.past_race.sort_values([target, 'Date'], ascending=False).reset_index(drop=True)
        df, dict_categorical_list, col_categorical = self.categorical_to_label(self.past_race, target, result_col_list)
        # df = self.past_race.copy()
        
        all_col_values = self.pd_to_np(df, target, result_col_list)
        # return all_col_values

        npdata = self.npdata_av(result_col_list, n_race, all_col_values)
        df2 = pd.DataFrame(npdata)

        col_list1 = [target, 'Date'] + [ f'{i}_{n_race}R' for i in result_col_list]
        df2.columns = col_list1
        df3 = self.label_to_categoriacal(df2, target, dict_categorical_list, col_categorical, n_race=n_race, av=True)
        # df3 = df2.copy()

        if today==False:
            df3['Date'] = pd.to_datetime(df3['Date'], format='%Y-%m-%d')
        if today==True:
            df3['Date'] = self.shutuba['Date'].unique()[0]
        return df3
        

    # 2. past_raceを条件別に集約して結合
    # [条件＋結果]の配列から，例）[Location, Condition,...Rank, SpdCoef,...]
    # 条件ごとの結果平均を算出  例）[LocationRank, LocationSpdCoef,...ConditionRank, ConditionSpdCoef,...]
    def calc_av_each_condition(self, past_npdata, condition_col_list, result_col_list):
        results_av_ec = np.full((1, len(condition_col_list)*len(result_col_list)),np.nan)[0]
        past_races_same_cond_idx = []
        # 同一条件の過去レースを検索
        for i in range(len(condition_col_list)):
            race_cond = past_npdata[i,0]
            past_races_same_cond_idx.append(np.where(past_npdata[i,] == race_cond)[0][1:])
        # 同一条件の過去レースの結果を平均
        i = 0
        for idx in past_races_same_cond_idx:
            j = i * len(result_col_list)
            if len(idx) > 0:
                try:
                    results_av = np.nanmean(past_npdata[len(condition_col_list):, idx], axis=1)
                    results_av_ec[j:j+len(result_col_list)] = results_av
                    i += 1
                except ZeroDivisionError:
                    i += 1
                    continue
            else:
                i += 1
        return results_av_ec
    
    # 過去n_race分の平均データを横に結合した２次元numpy配列を作成（同一条件に限る）
    # 始めの２列はtargetと'Date'
    # 例）[[target, Date, Rank, SpdCoef,..],
    #      [target, Date, Rank, SpdCoef,..],..]
    def npdata_av_ec(self, condition_col_list, result_col_list, n_race, all_col_values):
        horse_data_counts = collections.Counter(all_col_values[0])
        nrow = len(all_col_values[0])
        ncol = len(condition_col_list)*len(result_col_list) + 2
        npdata = np.full((nrow, ncol),np.nan)
        date = all_col_values[1]
        m = 0
        for k, v in horse_data_counts.items():
            for j in range(v):
                npdata[m,0] = k
                npdata[m,1] = date[m]
                n_race1 = v - (j + 1)
                if n_race1 >= n_race:
                    n_race1 = n_race
                past_npdata = all_col_values[ 2:, m : m+n_race1+1 ]
                if n_race1 > 0:
                    results_av_ec = self.calc_av_each_condition(past_npdata, condition_col_list, result_col_list)
                    npdata[m, 2 : ncol] = results_av_ec
                m += 1
        return npdata
    
    def merge_av_ec_all(self, target, condition_col_list, result_col_list, n_race=100, today=False):
        results_error_col_list = [result_col for result_col in result_col_list if self.past_race[result_col].dtypes != np.dtype(float)]
        if len(results_error_col_list) > 0:
            raise TypeError(results_error_col_list, "result_col_listにfloat以外のデータ型が含まれています")

        col_list = condition_col_list + result_col_list
        self.past_race = self.past_race.sort_values([target, 'Date'], ascending=False).reset_index(drop=True)
        df, dict_categorical_list, col_categorical = self.categorical_to_label(self.past_race, target, col_list)
        
        # horse_id = dict_categorical_list['HorseID']['2015104961'] # test用
        # all_col_values = self.pd_to_np(df, col_list, [horse_id])
        all_col_values = self.pd_to_np(df, target, col_list)
        npdata = self.npdata_av_ec(condition_col_list, result_col_list, n_race, all_col_values)
        df2 = pd.DataFrame(npdata)

        col_list1 = [target, 'Date']
        for i in condition_col_list:
            for j in result_col_list:
                col = i + j
                col_list1 += [col]
        df2.columns = col_list1
        col_categorical1 = [i for i in col_categorical if i in col_list1]
        df3 = self.label_to_categoriacal(df2, target, dict_categorical_list, col_categorical1, n_race=n_race, av=True)

        if today==False:
            df3['Date'] = pd.to_datetime(df3['Date'], format='%Y-%m-%d')
        if today==True:
            df3['Date'] = self.shutuba['Date'].unique()[0]
        return df3


    # 3. past_raceをそのまま結合
    # 過去n_race分のデータを横に結合した２次元numpy配列を作成
    # 始めの２列は'HorseID'と'Date'
    # 例）[[Location, Condition,...Rank, SpdCoef,...],
    #      [Location, Condition,...Rank, SpdCoef,...],...]
    def npdata_past_race(self, n_race, all_col_values):
        col_length = len(all_col_values) - 1
        horse_data_counts = collections.Counter(all_col_values[0])
        nrow = len(all_col_values[0])
        ncol = col_length * n_race + 2
        npdata = np.full((nrow, ncol),np.nan)
        date = all_col_values[1]
        all_col_values1 = all_col_values[1:]
        m = 0
        for k, v in horse_data_counts.items():
            for j in range(v):
                npdata[m,0] = k
                npdata[m,1] = date[m]
                n_race1 = v - (j + 1)
                if n_race1 >= n_race:
                    n_race1 = n_race
                horse_values = all_col_values1.T[m+1 : m+1 + n_race1, ].reshape(1, col_length * n_race1)[0]
                npdata[m, 2 : 2 + col_length * n_race1] = horse_values
                m += 1
        return npdata

    # 過去n_race分のデータを横に結合した２次元numpy配列を作成（当日用）
    # 当日の出走馬分の過去データのみ抽出
    def npdata_past_today_race(self, n_race, all_col_values):
        col_length = len(all_col_values) - 1
        horse_data_counts = collections.Counter(all_col_values[0])
        nrow = len(self.shutuba)
        ncol = col_length * n_race + 2
        npdata = np.full((nrow, ncol),np.nan)
        all_col_values1 = all_col_values[1:]
        m = 0
        j = 0
        for k, v in horse_data_counts.items():
            npdata[m,0] = k
            npdata[m,1] = 0
            n_race1 = v
            if n_race1 >= n_race:
                n_race1 = n_race
            horse_values = all_col_values1.T[j : j + n_race1, ].reshape(1, col_length * n_race1)[0]
            npdata[m, 2 : 2 + col_length * n_race1] = horse_values
            m += 1
            j += v
        return npdata
        
    # past_raceから過去レースを取得してそのまま結合する
    def merge_past_race(self, col_list, n_race=3, today=False):
        target = 'HorseID'
        if today==False:
            self.past_race = self.past_race.sort_values([target, 'Date'], ascending=False).reset_index(drop=True)
            df, dict_categorical_list, col_categorical = self.categorical_to_label(self.past_race, target, col_list)
            all_col_values = self.pd_to_np(df, target, col_list)
            npdata = self.npdata_past_race(n_race, all_col_values)
        if today==True:
            df = self.past_race.loc[self.past_race[target].isin(self.shutuba[target]), ]
            df = df.sort_values([target, 'Date'], ascending=False).reset_index(drop=True)
            df1, dict_categorical_list, col_categorical = self.categorical_to_label(df, target, col_list)
            all_col_values = self.pd_to_np(df1, target, col_list)
            npdata = self.npdata_past_today_race(n_race, all_col_values)

        col_list1 = [target, 'Date']
        for i in range(n_race):
          col_list1 += [ j + str(i+1) for j in ['Date'] + col_list]
        df2 = pd.DataFrame(npdata)
        df2.columns = col_list1
        col_categorical1 = [i for i in col_categorical if i in col_list1]
        df3 = self.label_to_categoriacal(df2, target, dict_categorical_list, col_categorical1, col_list=col_list1, n_race=n_race, av=False)
        
        for i in range(n_race):
          df3[f'Date{i+1}'] = pd.to_datetime(df3[f'Date{i+1}'], format='%Y-%m-%d')
        if today==False:
            df3['Date'] = pd.to_datetime(df3['Date'], format='%Y-%m-%d')
        if today==True:
            df3['Date'] = self.shutuba['Date'].unique()[0]
        return df3
    
    # rest作成
    def merge_rest(self, n_race=3, with_date=False, today=False, bin=True):
        n_race += 1
        df = self.merge_past_race([], n_race, today)
        date_list = [ i for i in list(df.columns) if 'Date' in str(i) ]
        for i in range(len(date_list)-1):
            if i == 0:
                rest = 'Rest'
            else:
                rest = 'Rest' + str(i)
            df[rest] = df[date_list[i]] - df[date_list[i+1]]
            df[rest] = df[rest].dt.days
          
        if with_date==False:
            df.drop(date_list[1:], axis=1, inplace=True)
        
        if bin==True:
            # binnning(2週間，1ヶ月半, 3ヶ月，それ以上)
            rest_list = [ i for i in list(df.columns) if 'Rest' in str(i) ]
            df_bin = df.copy()
            for i in rest_list:
                df_bin = df_bin.rename(columns={i:f'Bin{i}'})
                df_bin.loc[(df_bin[f'Bin{i}'] <= 16), f'Bin{i}'] = 0
                df_bin.loc[(df_bin[f'Bin{i}'] > 16) & (df_bin[f'Bin{i}'] <= 45), f'Bin{i}'] = 1
                df_bin.loc[(df_bin[f'Bin{i}'] > 45) & (df_bin[f'Bin{i}'] <= 60), f'Bin{i}'] = 2
                df_bin.loc[(df_bin[f'Bin{i}'] > 60), f'Bin{i}'] = 3
            return df_bin
        return df

    
    # 4. その他
    # 血統データ結合
    def merge_peds(self):
        self.shutuba = pd.merge(self.shutuba, self.peds, on='HorseID', how='left')
        self.past_race = pd.merge(self.past_race, self.peds, on='HorseID', how='left')

    # 調教データ結合
    def merge_train(self):
        train_all1 = self.train.copy()
        train_all1.sort_values(['Date'], ascending=False, inplace=True)
        train_all1.drop_duplicates(['HorseID', 'RaceID'], inplace=True)
        train_all1.reset_index(drop=True, inplace=True)
        train_all1.loc[:, 'Date'] = pd.to_datetime(train_all1['Date'], format='%Y/%m/%d')
        train_all1.rename(columns={
            'Date': 'TDate',
            'Course': 'TCourse',
            'Condition': 'TCondition',
            'Jockey': 'TJockey',
        }, inplace=True)

        self.shutuba = pd.merge(self.shutuba, train_all1, on=['HorseID', 'RaceID'], how='left')
        self.past_race = pd.merge(self.past_race, train_all1, on=['HorseID', 'RaceID'], how='left')

    # LastCRank列を作成
    def merge_lastc_rank(self):
        self.past_race['LastCRank'] = np.nan
        self.past_race.loc[(~self.past_race['2CRank'].isnull()) & (self.past_race['3CRank'].isnull()) & (self.past_race['Rank'] != 100), 'LastCRank'] = self.past_race['2CRank']
        self.past_race.loc[(~self.past_race['3CRank'].isnull()) & (self.past_race['4CRank'].isnull()) & (self.past_race['Rank'] != 100), 'LastCRank'] = self.past_race['3CRank']
        self.past_race.loc[(~self.past_race['4CRank'].isnull()) & (self.past_race['Rank'] != 100), 'LastCRank'] = self.past_race['4CRank']

        # 最終コーナーとゴールの両方で５着以内の競走馬の数
        both_in5_dict = self.past_race.loc[(self.past_race['LastCRank'] <= 5) & (self.past_race['Rank'] <= 5), ].groupby('RaceID')['HorseID'].count().to_dict()
        notboth_in5_raceid = self.past_race.loc[~self.past_race['RaceID'].isin(both_in5_dict.keys()), 'RaceID'].unique()
        for raceid in notboth_in5_raceid:
          both_in5_dict[raceid] = 0
        self.past_race['BothIn5'] = self.past_race['RaceID'].map(lambda x: np.float(both_in5_dict[x]))
    
    # データ型がobjectである場合，数値に置き換える
    def categorical_to_label(self, df, target, col_list):
        df1 = df.copy()
        df1['Date'] = df1['Date'].map(lambda x: str(x)[:10])
        col_list1 = [target, 'Date'] + col_list
        col_categorical = df1.loc[:, col_list1].select_dtypes(exclude=float).columns
        dict_categorical_list = {}
        for colname in col_categorical:
            label = np.arange(0, len(df1[colname].unique()), dtype=np.float32)
            dict_categorical = dict(zip(df1[colname].unique(), label))
            dict_categorical_list[colname] = dict_categorical

        for colname in col_categorical:
            df1[colname] = df1[colname].map(lambda x: self.change_value_by_dict(x=x, dict_value=dict_categorical_list[colname]))
        return df1, dict_categorical_list, col_categorical

    # 数値に置き換えたデータを元の値に戻す
    def label_to_categoriacal(self, df, target, dict_categorical_list, col_categorical, n_race, av=False, col_list=None):
        df1 = df.copy()
        dict_categorical_list1 = {}
        for colname, dict_categorical in dict_categorical_list.items():
            dict_categorical_list1[colname] = {v: k for k, v in dict_categorical.items()}

        for colname in col_categorical:
            df1[colname] = df1[colname].map(lambda x: self.change_value_by_dict(x=x, dict_value=dict_categorical_list1[colname]))

        if av == False:
            col_categorical1 = [i for i in col_categorical if (i in col_list) & (i not in [target, 'Date'])]
            for colname in col_categorical1:
                for j in range(1, n_race+1):
                    df1[colname+str(j)] = df1[colname+str(j)].map(lambda x: self.change_value_by_dict(x=x, dict_value=dict_categorical_list1[colname]))
        return df1
    
    # 各レース内での相対値に標準化，正則化する
    def scaling(self, df, col):
        standard_scaler = lambda x: (x - x.mean()) / x.std() # 標準化
        df[col] = df.groupby('RaceID')[col].transform(standard_scaler)
        df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min()) # min-maxスケーリング
        return df


    # past_raceを集約して結合(pandas)
    def average_pd(self, col_list, horse_id_list, date, n_samples='all'):
        target_df = self.past_race.query('HorseID in @horse_id_list')
        # print(target_df)
        
        #過去何走分取り出すか指定
        if n_samples == 'all':
              filtered_df = target_df[target_df['Date'] < date]
        elif n_samples > 0:
              filtered_df = target_df[target_df['Date'] < date].\
              sort_values('Date', ascending=False).groupby(['HorseID']).head(n_samples)
        else:
              raise Exception('n_samples must be >0')
        
        average = filtered_df.groupby(['HorseID'])[col_list].mean()
        for colname in col_list:
            average = average.rename(columns={colname:f'{colname}_{n_samples}R'})
        return average.reset_index()

    def merge_av_pd(self, col_list, date, n_samples_list=['all']):
        merged_df = self.shutuba.loc[self.shutuba['Date']==date, ['HorseID', 'Date']]
        horse_id_list = merged_df['HorseID']
        for n_samples in n_samples_list:
            merged_df = pd.merge(merged_df, self.average_pd(col_list, horse_id_list, date, n_samples), on='HorseID', how='left')
        return merged_df.reset_index(drop=True)

    def merge_av_all_pd(self, col_list, n_samples_list=['all']):
        date_list = self.shutuba['Date'].unique()
        merged_df = pd.concat([self.merge_av_pd(col_list, date, n_samples_list) for date in date_list])
        return merged_df.reset_index(drop=True)
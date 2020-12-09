import csv
import pandas as pd
import numpy as np
import datetime
import time
import re
from tqdm import tqdm
from selenium import webdriver
import chromedriver_binary
from bs4 import BeautifulSoup
import bs4
import os
from CommonFunction import CommonFunction


### クラス，関数定義
# 親クラス
class ScrapeTable(CommonFunction):
    def __init__(self, horse=False, peds=False, train=False, horse_df_ids=[1,3], colab=False):
        self.horse = horse
        self.peds = peds
        self.train = train
        if self.horse==True:
            if 1 in horse_df_ids:
                self.horse_info_df = pd.DataFrame()
            if 3 in horse_df_ids:
                self.past_race_df = pd.DataFrame()
        if self.peds==True:
            self.peds_df = pd.DataFrame()
            self.peds_id_name_df = pd.DataFrame()
        if self.train==True:
            self.train_df = pd.DataFrame()
        self.horse_df_ids = horse_df_ids
        self.colab = colab


    # driver起動
    def open_driver(self, headless=True):
        # driver起動
        options = webdriver.ChromeOptions()
        if headless==True:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        driver = webdriver.Chrome(options=options)
        return driver


    # driver起動 & ログイン
    def driver_with_login(self, lid, pwsd, headless=True):
        driver = self.open_driver(headless)
        # ログイン
        url = 'https://race.netkeiba.com/race/newspaper.html?m=riot-shutuba-past&race_id=201908030411'
        driver.get(url)
        time.sleep(5)
        element = driver.find_element_by_class_name("Premium_Regist_Btn")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") #ページ下部まで移動
        element.click()
        
        time.sleep(5)
        driver.find_element_by_class_name("restoreLink").click()

        time.sleep(5)
        driver.find_element_by_name("login_id").send_keys(lid)
        driver.find_element_by_name("pswd").send_keys(pwsd)
        driver.find_element_by_xpath("/html/body/div[1]/div/div[1]/div[1]/div[2]/form/input[5]").click()
        print('ログイン完了')
        return driver


    # 競走馬基本情報，過去レーススクレイピング
    def scrape_horse(self, driver, horse_id_list, error_file):
        scrape_data = {}
        for horse_id in tqdm (horse_id_list):
            # print(horse_id)
            try:
                url = 'https://db.netkeiba.com/horse/' + str(horse_id)
                driver.get(url)
                time.sleep(2)
                content = driver.page_source.encode('utf-8')
                soup = BeautifulSoup(content, "html.parser")
                data = pd.read_html(str(soup))

                #調教師ID，馬主ID，生産者IDをスクレイピング
                trainer_tag = soup.find("table", attrs={"summary": "のプロフィール"}).find("a", attrs={"href": re.compile(r"^/trainer/")})
                owner_tag = soup.find("table", attrs={"summary": "のプロフィール"}).find("a", attrs={"href": re.compile(r"^/owner/")})
                breeder_tag = soup.find("table", attrs={"summary": "のプロフィール"}).find("a", attrs={"href": re.compile(r"^/breeder/")})
                trainer_id = tag_to_id(trainer_tag)
                owner_id = tag_to_id(owner_tag)
                breeder_id = tag_to_id(breeder_tag)

                #レースID、騎手IDをスクレイピング
                table_rows = soup.find("table", attrs={"class": "db_h_race_results"}).find_all("tr")
                race_id_list = []
                for row in table_rows[1:]:
                    race_id = row.find_all("td")[4].find("a", attrs={"href": re.compile(r"^/race/")})
                    race_id = race_id["href"].split('/')[2]
                    race_id_list.append(race_id)    
                jockey_id_list = []
                for row in table_rows[1:]:
                    jockey_id = row.find_all("td")[12].find("a", attrs={"href": re.compile(r"^/jockey/")})
                    if jockey_id != None:
                        jockey_id = jockey_id["href"].split('/')[2]
                    jockey_id_list.append(jockey_id)
                
                #データ格納
                if (data[1].iloc[0,0] == '生年月日') & (data[3].columns[0] == '日付'):
                    # データ格納
                    # print('data取得完了')
                    data[1] = data[1].T #行列入れ替え
                    horse_name = soup.find('div', class_='horse_title').find('h1').text.strip()
                    horse_name = re.findall(r"[A-z\u30A1-\u30FF]+", str(horse_name))[0]
                    data[1].insert(0, 'Name', horse_name)
                    data[1]['TrainerID'] = trainer_id
                    data[1]['OwnerID'] = owner_id
                    data[1]['BreederID'] = breeder_id
                    data[3]["RaceID"] = race_id_list
                    data[3]["JockeyID"] = jockey_id_list
                    scrape_data[horse_id] = [data[id] for id in self.horse_df_ids]
                    # print('data格納完了')
                elif (data[1].iloc[0,0] == '生年月日') & (data[4].columns[0] == '日付'):
                    # print('data取得完了')
                    data[1] = data[1].T #行列入れ替え
                    horse_name = soup.find('div', class_='horse_title').find('h1').text.strip()
                    horse_name = re.findall(r"[A-z\u30A1-\u30FF]+", str(horse_name))[0]
                    data[1].insert(0, 'Name', horse_name)
                    data[1]['TrainerID'] = trainer_id
                    data[1]['OwnerID'] = owner_id
                    data[1]['BreederID'] = breeder_id
                    data[4]["RaceID"] = race_id_list
                    data[4]["JockeyID"] = jockey_id_list
                    ids_ex = self.horse_df_ids.copy()
                    if 3 in ids_ex:
                        ids_ex[ids_ex.index(3)] = 4
                    scrape_data[horse_id] = [data[id] for id in ids_ex]
                    # print('data格納完了')
                else:
                    print('scrape_horse: データフレームの条件が一致しません。', horse_id)
                    self.save_csv([horse_id], error_file)
            except IndexError:
                print('scrape_horse: IndxError', horse_id)
                self.save_csv([horse_id], error_file)
            except AttributeError:
                print('scrape_horse: AttributeError', horse_id)
                self.save_csv([horse_id], error_file)
            except TypeError: # IDが無いときなどに生じる
                print('scrape_shutuba: TypeError', horse_id)
                self.save_csv([race_id], error_file)
            # except:
            #     print('scrape_horse: 例外エラー', horse_id)
            #     self.save_csv([horse_id], error_file)
            #     time.sleep(1)
        return scrape_data

    # 血統
    def scrape_peds(self, driver, horse_id_list, error_file, peds_id_name_all=None, peds_id_name=False):
        self.peds_id_name = peds_id_name
        if self.peds_id_name==True:
            peds_id_all = [str(i) for i in peds_id_name_all['HorseID']]
        for horse_id in tqdm(horse_id_list):
            try:
                url = "https://db.netkeiba.com/horse/ped/" + str(horse_id)
                driver.get(url)
                time.sleep(2)
                content = driver.page_source.encode('utf-8')
                soup = BeautifulSoup(content, "html.parser")
                peds_a_list = soup.find("table", attrs={"summary": "5代血統表"}).find_all("a", attrs={"href": re.compile(r"^/horse/\d")}) # \dを付けないと，余分な情報[血統][産駒]といったリンクまで取得してしまう。
                peds_id_list = []
                #print('スクレイピング完了')
                for a in peds_a_list:
                    peds_name = re.sub('\n', '', a.text)
                    peds_id = a["href"].split('/')[2]
                    peds_id_list.append(peds_id)
                    #print(peds_id, peds_name)
                    if self.peds_id_name==True:
                        if peds_id not in peds_id_all:
                            peds_id_all.append(peds_id)
                            #print(peds_id, peds_name)
                            self.peds_id_name_df = self.peds_id_name_df.append(pd.Series([peds_id, peds_name]), ignore_index=True)
                self.peds_df = self.peds_df.append(pd.Series([str(horse_id)] + peds_id_list), ignore_index=True)
                #print('血統取得完了', len(peds_id_list))
            except IndexError:
                print('scrape_peds: IndxError', horse_id)
                self.save_csv([horse_id], error_file)
            except:
                print('scrape_peds: 例外エラー', horse_id)
                self.save_csv([horse_id], error_file)
                time.sleep(1)

    # 調教
    def scrape_train(self, driver, horse_id_list, error_file):
        scrape_data = {}
        try:
            for horse_id in tqdm (horse_id_list):
                url = 'https://db.netkeiba.com/?pid=horse_training&id=' + str(horse_id)
                driver.get(url)
                time.sleep(2)
                content = driver.page_source.encode('utf-8')
                soup = BeautifulSoup(content, "html.parser")
                data = pd.read_html(str(soup).replace('</li>', 'li'))

                for i in range(0, len(data)):
                    caption = soup.find_all("table", attrs={"summary": "調教タイム"})[i].find("caption")
                    race_id = caption.find("a", attrs={"href": re.compile(r"^/race/")})["href"].split('/')[2]
                    data[i]['RaceID'] = race_id
                data1 = pd.concat(data)
                scrape_data[horse_id] = data1
        except IndexError:
            print('scrape_peds: IndxError', horse_id)
            self.save_csv([horse_id], error_file)
    #     except:
    #         print('scrape_peds: 例外エラー', horse_id)
    #         self.save_csv([horse_id], error_file)
    #         time.sleep(1)
        return scrape_data


    # データ結合
    def df_concat_horse(self, scrape_data):
        for key in scrape_data.keys():
            if 1 in self.horse_df_ids:
                id = self.horse_df_ids.index(1)
                scrape_data[key][id].index = [key] * len(scrape_data[key][id])
                self.horse_info_df =  pd.concat([self.horse_info_df, scrape_data[key][0]])
            if 3 in self.horse_df_ids:
                id = self.horse_df_ids.index(3)
                scrape_data[key][id].index = [key] * len(scrape_data[key][id])
                self.past_race_df =  pd.concat([self.past_race_df, scrape_data[key][id]])
    
    def df_concat_train(self, scrape_data):
        for key in scrape_data.keys():
            scrape_data[key].index = [key] * len(scrape_data[key])
            self.train_df = pd.concat([self.train_df, scrape_data[key]])


    # データ更新
    def df_update_horse(self, horse_info_all, past_race_all):
        if 1 in self.horse_df_ids:
            horse_id_list = past_race_all['HorseID'].unique()
            horse_id_new = self.horse_info_df.index.unique()
            horse_id_yet = [ i for i in horse_id_new if i not in horse_id_list]
            self.horse_info_df = self.horse_info_df.loc[self.horse_info_df.index.isin(horse_id_yet), ]

        if 3 in self.horse_df_ids:
            past_race_all['HorseID_RaceID'] = past_race_all['HorseID'] + past_race_all['RaceID']
            self.past_race_df['HorseID_RaceID'] = self.past_race_df.index.map(str) + self.past_race_df['RaceID']
            horse_id_date_list = past_race_all['HorseID_RaceID'].unique()
            horse_id_date_new = self.past_race_df['HorseID_RaceID'].unique()
            horse_id_date_yet = [ i for i in horse_id_date_new if i not in horse_id_date_list]
            self.past_race_df = self.past_race_df.loc[self.past_race_df['HorseID_RaceID'].isin(horse_id_date_yet), ]
            self.past_race_df = self.past_race_df.drop(['HorseID_RaceID'], axis=1)

    def df_update_train(self, train_all):
            train_all['HorseID_RaceID'] = train_all['HorseID'] + train_all['RaceID']
            self.train_df['HorseID_RaceID'] = self.train_df.index.map(str) + self.train_df['RaceID']
            horse_id_race_id_list = train_all['HorseID_RaceID'].unique()
            horse_id_race_id_new = self.train_df['HorseID_RaceID'].unique()
            horse_id_race_id_yet = [ i for i in horse_id_race_id_new if i not in horse_id_race_id_list]
            self.train_df = self.train_df.loc[self.train_df['HorseID_RaceID'].isin(horse_id_race_id_yet), ]
            self.train_df = self.train_df.drop(['HorseID_RaceID'], axis=1)

    def df_update_peds(self, peds_all):
            horse_id_list = peds_all['HorseID'].unique()
            horse_id_new = self.peds_df.index.unique()
            horse_id_yet = [ i for i in horse_id_new if i not in horse_id_list]
            self.peds_df = self.peds_df.loc[self.peds_df.index.isin(horse_id_yet), ]
    
    # csv保存
    def df_to_csv_horse_peds(self, dir_name, file_id, mode='a', dir_name_database='Database', database_mode='a', header=False):
        if self.colab == True:
            dir_name1 = f'drive/My Drive/keiba/{dir_name}'
            dir_name_database1 = f'drive/My Drive/keiba/{dir_name_database}'
        else:
            dir_name1 = dir_name
        if self.horse == True:
            if 1 in self.horse_df_ids:
                self.horse_info_df.to_csv(f'{dir_name1}/horse_info{file_id}.csv', mode=mode, header=header)
            if 3 in self.horse_df_ids:
                self.past_race_df.to_csv(f'{dir_name1}/past_race{file_id}.csv', mode=mode, header=header)
        if self.peds == True:
            self.peds_df.to_csv(f'{dir_name1}/peds{file_id}.csv', mode=mode, header=header)
            if self.peds_id_name==True:
                self.peds_id_name_df.to_csv(f'{dir_name_database1}/peds_id_name.csv', mode=database_mode, header=header)
        if self.train == True:
            self.train_df.to_csv(f'{dir_name1}/train{file_id}.csv', mode=mode, header=header)


###########################################################################################################################
# 出馬表(結果データベース)スクレイピングクラス
class ScrapePostRace(ScrapeTable):
    def __init__(self, horse=False, peds=False, train=False, shutuba=False, horse_df_ids = [1,3], shutuba_df_ids = [0,1,2,4,5], colab=False):
        super().__init__(horse=horse, peds=peds, train=train, horse_df_ids=horse_df_ids, colab=colab)
        self.shutuba = shutuba
        if self.shutuba==True:
            self.shutuba_df = pd.DataFrame()
            self.payouts_df = pd.DataFrame()
            self.corner_rank_df = pd.DataFrame()
            self.lap_time_df = pd.DataFrame()
            self.shutuba_df_ids = shutuba_df_ids
    

    # 出馬表（結果データベース）
    def scrape_shutuba(self, driver, race_id_list, error_file):
        scrape_data = {}
        for race_id in tqdm(race_id_list):    
            try:
                url = 'https://db.netkeiba.com/race/' + str(race_id)
                driver.get(url)
                time.sleep(2)
                content = driver.page_source.encode('utf-8')
                soup = BeautifulSoup(content, "html.parser")
                data = pd.read_html(str(soup).replace('<br/>', 'br'))
                if (data[0].columns[0] == '着br順') & (data[1].iloc[0,0] == '単勝') & ('コーナー' in data[4].iloc[0,0]) & ('ラップ' in data[5].iloc[0,0]):

                    #馬ID、騎手IDをスクレイピング
                    horse_id_list = []
                    horse_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all("a", attrs={"href": re.compile(r"/horse/\d.+")})
                    for a in horse_a_list:
                        horse_id = a["href"].split('/')[2]
                        horse_id_list.append(horse_id)
                    data[0]["HorseID"] = horse_id_list
                    jockey_id_list = []
                    jockey_a_list = soup.find("table", attrs={"summary": "レース結果"}).find_all("a", attrs={"href": re.compile(r"/jockey/")})
                    for a in jockey_a_list:
                        jockey_id = a["href"].split('/')[2]
                        jockey_id_list.append(jockey_id)
                    data[0]["JockeyID"] = jockey_id_list
                    # データ格納
                    scrape_data[race_id] = [data[id] for id in self.shutuba_df_ids]
            except IndexError:
                print('scrape_shutuba: IndxError', race_id)
                self.save_csv([race_id], error_file)
            # except:
            #     print('scrape_shutuba: 例外エラー', race_id)
            #     self.save_csv([race_id], error_file)
            #     time.sleep(1)
        return scrape_data


    # データ結合
    def df_concat_shutuba(self, scrape_data):
        for key in scrape_data.keys():
            if 0 in self.shutuba_df_ids:
                id = self.shutuba_df_ids.index(0)
                scrape_data[key][id].index = [key] * len(scrape_data[key][id])
                self.shutuba_df = pd.concat([self.shutuba_df, scrape_data[key][id]])
            if (1 in self.shutuba_df_ids) & (2 in self.shutuba_df_ids):
                id1 = self.shutuba_df_ids.index(1)
                id2 = self.shutuba_df_ids.index(2)
                scrape_data[key][id1].index = [key] * len(scrape_data[key][id1])
                scrape_data[key][id2].index = [key] * len(scrape_data[key][id2])
                self.payouts_df = pd.concat([self.payouts_df, scrape_data[key][id1], scrape_data[key][id2]])
            if 4 in self.shutuba_df_ids:
                id = self.shutuba_df_ids.index(4)
                scrape_data[key][id].index = [key] * len(scrape_data[key][id])
                self.corner_rank_df = pd.concat([self.corner_rank_df, scrape_data[key][id]])
            if 5 in self.shutuba_df_ids:
                id = self.shutuba_df_ids.index(5)
                scrape_data[key][id].index = [key] * len(scrape_data[key][id])
                self.lap_time_df = pd.concat([self.lap_time_df, scrape_data[key][id]])
         
    # csv保存
    def df_to_csv_shutuba(self, dir_name, file_id, mode='a', header=False):
        if self.colab == True:
            dir_name1 = f'drive/My Drive/keiba/{dir_name}'
        else:
            dir_name1 = dir_name
        if self.shutuba == True:
            if 0 in self.shutuba_df_ids:
                self.shutuba_df.to_csv(f'{dir_name1}/shutuba{file_id}.csv', mode=mode, header=header)
            if (1 in self.shutuba_df_ids) & (2 in self.shutuba_df_ids):
                self.payouts_df.to_csv(f'{dir_name1}/payouts{file_id}.csv', mode=mode, header=header)
            if 4 in self.shutuba_df_ids:
                self.corner_rank_df.to_csv(f'{dir_name1}/corner_rank{file_id}.csv', mode=mode, header=header)
            if 5 in self.shutuba_df_ids:
                self.lap_time_df.to_csv(f'{dir_name1}/lap_time{file_id}.csv', mode=mode, header=header)
    
    def df_to_csv_all(self, dir_name, file_id, mode='a', dir_name_database='Database', database_mode='a', header=False):
        self.df_to_csv_shutuba(dir_name, file_id, mode)
        self.df_to_csv_horse_peds(dir_name=dir_name, file_id=file_id, mode=mode, dir_name_database=dir_name_database, database_mode=database_mode, header=header)

        
###########################################################################################################################
# 当日の出馬表スクレイピングクラス
class ScrapeTodayRace(ScrapeTable):
    def __init__(self, today_race=False, horse=False, peds=False, train=False, horse_df_ids=[1,3], colab=False):
        super().__init__(horse=horse, peds=peds, train=train, horse_df_ids=horse_df_ids, colab=colab)
        self.today_race = today_race
        if self.today_race==True:
            self.today_race_df = pd.DataFrame()


    # 出馬表スクレイピング
    def scrape_today(self, driver, race_id_list):
        scrape_data = {}
        for race_id in tqdm(race_id_list):    
            # try:
            url = f'https://race.netkeiba.com/race/shutuba.html?race_id={race_id}'
            driver.get(url)
            time.sleep(2)
            content = driver.page_source.encode('utf-8')
            soup = BeautifulSoup(content, "html.parser")
            data = pd.read_html(str(soup))
            
            #馬ID、騎手IDをスクレイピング
            horse_id_list = []
            horse_a_list = soup.find("table", attrs={"class": "Shutuba_Table"}).find_all("a", attrs={"href": re.compile(r"/horse/\d.+")})
            for a in horse_a_list:
                horse_id = a["href"].split('/')[4]
                horse_id_list.append(horse_id)
            data[0]["HorseID"] = horse_id_list
            jockey_id_list = []
            jockey_a_list = soup.find("table", attrs={"class": "Shutuba_Table"}).find_all("a", attrs={"href": re.compile(r"/jockey/\d.+")})
            for a in jockey_a_list:
                jockey_id = a["href"].split('/')[4]
                jockey_id_list.append(jockey_id)
            data[0]["JockeyID"] = jockey_id_list

            # レース情報取得
            year = datetime.datetime.today().strftime('%Y')
            date = datetime.datetime.today().strftime('%Y/%m/%d')
            race_info01 = soup.find("div", attrs={"class": "RaceData01"}).find_all("span")
            race_info02 = soup.find("div", attrs={"class": "RaceData02"}).find_all("span")
            race_info01_tx = [ i.text for i in race_info01 ]
            race_info02_tx = [ i.text for i in race_info02 ]
            data[0]['Year'] = year
            data[0]['Date'] = date
            data[0]['Number'] =re.sub("\\D", "", race_info02_tx[7])
            data[0]['Location'] = race_info02_tx[1]
            data[0]['Term'] = str(race_id)[-6:-4]
            data[0]['Day'] = str(race_id)[-4:-2]
            data[0]['R'] = str(race_id)[-2:]
            data[0]['Racetype_Length'] = race_info01_tx[0]
            data[0]['Condition'] = race_info01_tx[2][-1]
            scrape_data[race_id] = data[0]
        return scrape_data

    # 未取得の競走馬id特定
    def find_horse_id_yet(self, horse_id_list):
          horse_id_today = self.today_race_df['HorseID'].unique()
          horse_id_yet = [ i for i in horse_id_today if i not in horse_id_list]
          return horse_id_yet
    
    # データ結合
    def df_concat_today(self, scrape_data):
        for key in scrape_data.keys():
            scrape_data[key].index = [key] * len(scrape_data[key])
            self.today_race_df = pd.concat([self.today_race_df, scrape_data[key]])

    # 保存
    def df_to_csv_today(self, dir_name, file_id='', mode='a', header=False):
        if self.colab == True:
            dir_name1 = f'drive/My Drive/keiba/{dir_name}'
        else:
            dir_name1 = dir_name
        if self.today_race==True:
            self.today_race_df.to_csv(f'{dir_name1}/today_race{file_id}.csv', mode=mode, header=header)
    
    def df_to_csv_all(self, dir_name, file_id='', mode='a', dir_name_database='Database', database_mode='a', header=False):
        self.df_to_csv_today(dir_name, file_id, mode)
        self.df_to_csv_horse_peds(dir_name=dir_name, file_id=file_id, mode=mode, dir_name_database=dir_name_database, database_mode=database_mode, header=header)

    # レースID作成
    def make_raceid_list(self, bgn_race=7, fin_race=12):
        race_id_list = []
        for year in range(2020, 2006, -1):
            for place in range(1,11,1):
                for kai in range(1,6,1):
                    for day in range(1,11,1):
                        for r in range(bgn_race,fin_race+1,1):
                            race_id_list.append(str(year) + str(place).zfill(2) + str(kai).zfill(2) + str(day).zfill(2) + str(r).zfill(2)) 
        return race_id_list

## 必要関数定義
def tag_to_id(tag):
    if tag == None:
        id = None
    else:
        id = tag["href"].split('/')[2]
    return id
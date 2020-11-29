import numpy as np
import csv

class CommonFunction:
    def change_value_by_dict(self, x, dict_value, NAN = True):
        try:
            x = dict_value[x]
            return x
        except KeyError:
            if NAN == True:
                return np.nan
            else:
                return x

    def int_to_str(self, x, zfill_dig=None):
        try:
            if zfill_dig != None :
                x1 = x1.zfill(zfill_dig)
            return x1
        except:
            return None

    def to_float(self, x):
        try:
            return float(x)
        except:
            return np.nan

    # csv関連
    # 1行保存
    def save_csv(self, data, file_name, mode='a'):
        with open(file_name, mode, encoding = 'utf-8_sig') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(data)

    # リストcsv書き込み
    def list_to_csv(self, data_list, file_name, mode='a'):
        with open(file_name, mode, encoding = 'utf-8') as f:
            writer = csv.writer(f)
            for data in data_list:
                writer.writerow([data])

    # リストcsv読み込み
    def list_from_csv(self, file_name):
        data_list = []
        with open(file_name, encoding="utf-8") as f:
            reader = csv.reader(f)
            for data in reader:
                data_list.append(data[0])
        return data_list


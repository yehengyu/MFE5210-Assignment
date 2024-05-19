import os
from PIL import Image

import pandas as pd

import datetime
import os

import pymysql
import numpy as np

import dolphindb as ddb
import math

from sklearn.linear_model import LinearRegression # 为了之后进行线性回归而做的



import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体为SimHei
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

import warnings
warnings.filterwarnings('ignore')
#from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
from datetime import datetime


import os
path = 'F:\\实习生叶恒宇\\超预期研报复现'
os.chdir(path)
import common as mycomm


next_rtndf =pd.read_csv('next_rtndf.csv')
next_rtndf.set_index (next_rtndf.TRADE_DATE,inplace = True)
next_rtndf = next_rtndf.iloc[:,1:]
c1rtndf = mycomm.cutDF(next_rtndf, "2009-01-01", "2024-03-10")
c1rtndf.replace(0,np.nan,inplace = True)
c2rtndf = mycomm.cutDF(next_rtndf, "20091231", "20240310")
c2rtndf.replace(0,np.nan,inplace = True)
################################################################################
mycomm.connect_toABM()
query = "SELECT stock_code,report_type,declare_date,report_quarter,report_year,np_shareholder FROM abmdata.fin_income_gen \
    WHERE report_type LIKE '1001'  ORDER BY declare_date,stock_code,report_year,report_quarter;"
df1 = mycomm.getDTFbySQL(mycomm.connectflag, query, ['stock_code','report_type','declare_date','report_quarter','report_year','np_shareholder'])
df1 = df1.drop_duplicates(subset=['stock_code','declare_date','report_quarter','report_year'])
# 扩充表格index，使得表格完整
tdf1 = pd.DataFrame(df1.stock_code.unique())
tdf2= pd.DataFrame(df1.report_year.unique())
tdf3 = pd.DataFrame(df1.report_quarter.unique())
## 添加一个虚拟键，以便执行笛卡尔积
tdf1['key'] = 1
tdf2['key'] = 1
## 执行笛卡尔积操作
full_ind = pd.merge(tdf1, tdf2, on='key').drop('key', axis=1)
full_ind['key'] = 1
tdf3['key']     = 1
full_ind = pd.merge(full_ind,tdf3,on = 'key').drop('key',axis = 1)
# 继续清洗数据
full_ind.columns = ['stock_code','report_year','report_quarter']
df11 = pd.merge(df1,full_ind,left_on = ['stock_code','report_year','report_quarter'],right_on = ['stock_code','report_year','report_quarter'],how = 'right')
df11=df11.sort_values(['stock_code','report_year','report_quarter'])
df11.report_type = df11.report_type.astype(str)
df11.report_quarter = df11.report_quarter.astype(str)
df11.report_year = df11.report_year.astype(str)
df11['declare_date'] = df11['declare_date'].astype(str)
df11[['year','mon','day']] = df11['declare_date'].str.split('-',expand = True)
df11.declare_date = pd.to_datetime(df11.declare_date)

# 按照研报计算因子
df11['chushu'] = df11.report_quarter.astype(int)-1000
df11.chushu = df11.chushu.astype(float)
df11['np'] = df11.np_shareholder/df11.chushu

def SUE_group(group):
    group_std = (group - ((((group - group.shift(1))/group.shift(1)).rolling(window = 7,min_periods = 4).mean() +1)
                          * group).shift(1))/(group.rolling(window = 8,min_periods = 4).std().shift(1))
    return group_std
new_df2 = df11.set_index(['report_year','report_quarter']).sort_values(['stock_code','report_year','report_quarter']).groupby(['stock_code'])['np'].apply(SUE_group)

# new_df2.to_csv('yhy_SUE.csv')
SUE = new_df2
tdf  = df1[['stock_code','report_quarter','report_year','declare_date']]
SUE1 = pd.merge(SUE,tdf,left_on =['stock_code','report_quarter','report_year'],right_on = ['stock_code','report_quarter','report_year'],how = 'left')
SUE2 = SUE1.iloc[:,[0,3,4]]
SUE2.columns = ['stock_code','sue','date']

sue_df = mycomm.getEventCtoR(c1rtndf,SUE2,'date','stock_code','sue',method = "last",limitvalue = 0)
sui = sue_df.copy()
sui.replace(0,np.nan,inplace = True)
sui  = sui.fillna(method = 'ffill')


aaaa = mycomm.groupAgg2(sui,c2rtndf,n =10)

mycomm.show_pic_IC(sui,c2rtndf,period='M')

















# 获取当前Python文件的绝对路径
current_path = os.path.dirname(os.path.abspath(__file__))

# 打开要保存的图片
image = Image.open("image.jpg")

# 保存图片到当前文件夹
image.save(os.path.join(current_path, "output_image.jpg"))

print("图片已保存到当前文件夹")
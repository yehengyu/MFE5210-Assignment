import pandas as pd
import numpy as np
import os
path = 'F:/实习生叶恒宇/超预期研报复现'
os.chdir(path)
import datetime
from decimal import Decimal
import common as mycomm
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体为SimHei
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 导入收益率数据

next_rtndf = pd.read_csv('next_rtndf.csv')
next_rtndf.set_index('TRADE_DATE',inplace = True)
next_rtndf.index = pd.to_datetime(next_rtndf.index)
next_rtndf =  mycomm.cutDF(next_rtndf, "20090101", "20240310")
c1rtndf = mycomm.cutDF(next_rtndf, "20090101", "20240310")
c1rtndf.replace(0,np.nan,inplace = True)
c2rtndf = mycomm.cutDF(next_rtndf, "20090101", "20240310")
c2rtndf.replace(0,np.nan,inplace = True)

# 导入总市值数据df1
mycomm.connect_toABM()
q = 'select TICKER_SYMBOL,TRADE_DATE,MARKET_VALUE,NEG_MARKET_VALUE,PE from datayes.mkt_equd order by TICKER_SYMBOL,TRADE_DATE'
df1 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['TICKER_SYMBOL','TRADE_DATE','MARKET_VALUE','NEG_MARKET_VALUE','PE'])
mycomm.closeconnect()

df11 = mycomm.getEventCtoR(c1rtndf,df1,"TRADE_DATE","TICKER_SYMBOL","MARKET_VALUE",method = "sum")

from sklearn.preprocessing import StandardScaler


def std_l(df1):  # need from sklearn.preprocessing import StandardScaler
    df = df1.copy()
    scaler = StandardScaler()
    normalized_data = scaler.fit_transform(df.T).T  # 处理股票数据，所有用这个来改变标准化方向
    normalized_df = pd.DataFrame(normalized_data, columns=df.columns, index=df.index)
    df1 = normalized_df
    df1 = df1.mask((df > 3) | (df < -3), np.nan)
    return df1

#df11 = std_l(df11)
df11.replace(0,np.nan,inplace = True)
DF = 1/df11
DF = std_l(DF)

DF = 1/df11
DF = std_l(DF)

mycomm.groupAgg2(DF,c1rtndf)
mycomm.show_pic_IC(DF,c1rtndf)
import pandas as pd
import numpy as np
import os
path = 'F:\\实习生叶恒宇\\超预期研报复现'
os.chdir(path)

import common as mycomm

import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体为SimHei
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

next_rtndf =pd.read_csv('next_rtndf.csv')
next_rtndf.set_index (next_rtndf.TRADE_DATE,inplace = True)
next_rtndf = next_rtndf.iloc[:,1:]
c1rtndf = mycomm.cutDF(next_rtndf, "2009-01-01", "2024-03-10")
c1rtndf.replace(0,np.nan,inplace = True)

## 这里修改了一下数据来源，所以显得有点凌乱

## 获取上市公司披露预报的数据df5_1

## 获取上市公司披露快报的数据df5_2

## 获取上市公司披露财报的数据df5_3

mycomm.connect_toABM()
q ="SELECT TICKER_SYMBOL,ACT_PUBTIME,PUBLISH_DATE,END_DATE,REPORT_TYPE,FISCAL_PERIOD,N_INCOME_ATTR_P FROM datayes.vw_fdmt_is_new WHERE MERGED_FLAG =1 ORDER BY TICKER_SYMBOL,PUBLISH_DATE;"
df6_3 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','N_INCOME_ATTR_P'])
q = "SELECT TICKER_SYMBOL,ACT_PUBTIME,PUBLISH_DATE,END_DATE,REPORT_TYPE,FISCAL_PERIOD,N_INCOME_ATTR_P FROM datayes.fdmt_ee WHERE MERGED_FLAG =1 ORDER BY TICKER_SYMBOL,PUBLISH_DATE;"
df6_2 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','N_INCOME_ATTR_P'])
q = "SELECT TICKER_SYMBOL,ACT_PUBTIME,PUBLISH_DATE,END_DATE,REPORT_TYPE,FISCAL_PERIOD,EXPN_INCAP_LL FROM datayes.fdmt_ef_new WHERE MERGED_FLAG =1 ORDER BY TICKER_SYMBOL,PUBLISH_DATE ;"
df6_1 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','EXPN_INCAP_LL'])
mycomm.closeconnect()
# 写一个拆分act——pubtime的函数，并且做一个判断，将大于09:00:00的日子往后挪移一天，原数据框会得到拆分的actdate和acttime
def splitA_P(df1,col_name = "ACT_PUBTIME",comp_time = "09:00:00"):
    df = df1.copy()
    df[col_name] = df[col_name].astype(str)
    df[['actdate','acttime']] = df[col_name].str.split(' ',expand = True)
    df.actdate =  pd.to_datetime(df.actdate)
    cond = pd.to_datetime(df.acttime) > pd.to_datetime("09:00:00")
    df.loc[cond,'actdate'] += np.timedelta64(1,"D")
    return df
df6_3 = splitA_P(df6_3)
df6_1 = splitA_P(df6_1)
df6_2 = splitA_P(df6_2)

# 去除数据中以A开头的股票们
condition = ~df6_1['TICKER_SYMBOL'].str.startswith('A')
df6_1 = df6_1[condition]
condition = ~df6_3['TICKER_SYMBOL'].str.startswith('A')
df6_3 = df6_3[condition]
df5_1 = df6_1.sort_values(['TICKER_SYMBOL','actdate']).iloc[:,[0,-2,-4]]
df5_2 = df6_3.sort_values(['TICKER_SYMBOL','actdate']).iloc[:,[0,-2,-4]]
df5_3 = df6_3.sort_values(['TICKER_SYMBOL','actdate']).iloc[:,[0,-2,-4]]

# 获取交易日信息
mycomm.connect_toABM()
q = "SELECT distinct trade_date FROM abmdata.qt_stk_daily;"
trade_date =mycomm.getDTFbySQL(mycomm.connectflag, q, ['trade_date'])

# 导入收盘价和开盘价
#mycomm.connect_toABM()
## 获取收盘价数据 df4
q = "SELECT TICKER_SYMBOL,TRADE_DATE,OPEN_PRICE_2,CLOSE_PRICE_2 FROM datayes.mkt_equd_adj_af ORDER BY TICKER_SYMBOL,TRADE_DATE;"
df4 = mycomm.getDTFbySQL(mycomm.connectflag, q, ['TICKER_SYMBOL','TRADE_DATE','OPEN_PRICE_2','CLOSE_PRICE_2'])


# 导入中证500
#mycomm.connect_toABM()
q = "SELECT TRADE_DATE,OPEN_INDEX,CLOSE_INDEX FROM datayes.mkt_idxd_csi WHERE TICKER_SYMBOL=000905 ORDER BY TRADE_DATE;"
zz500 = mycomm.getDTFbySQL(mycomm.connectflag, q, ['TRADE_DATE','OPEN_INDEX','CLOSE_INDEX'])

df4['aog1'] = df4.OPEN_PRICE_2 /(df4.CLOSE_PRICE_2.shift(1))
zz500['aog1'] = zz500.OPEN_INDEX/(zz500.CLOSE_INDEX.shift(1))
AOG = df4.pivot_table(columns='TICKER_SYMBOL',index='TRADE_DATE',values='aog1').reset_index()

# 整理和剪切两者
AOG.set_index('TRADE_DATE',inplace = True)
AOG.index = pd.to_datetime(AOG.index)

AOG_df = mycomm.cutDF(AOG,"20090101","20240310")
zz500.set_index('TRADE_DATE',inplace = True)
zz500.index = pd.to_datetime(zz500.index)
zz500 = mycomm.cutDF(zz500,"20090101","20240310")

#做差
minus = pd.DataFrame(zz500.aog1)
all1 = np.ones((AOG_df.shape[1]))
minus = (minus.to_numpy().astype(float))  *all1

AOG_df = AOG_df- minus

# 整理df5，作为盈余公告披露日的原数据框
df5 = pd.concat([df5_1,df5_2])
df5 = pd.concat([df5,df5_3])

def fill_stkcd(arr):
    arr = pd.Series(list(arr))
    arr = arr.astype(str).str.zfill(6)
    return arr.tolist()

df5.columns = ['stock_code','declare_date','is_valid']

df5.declare_date = pd.to_datetime(df5.declare_date)

event0_df = mycomm.getEventCtoR(c1rtndf, df5,  'declare_date','stock_code', 'is_valid','count')

# 将周六周日发生的事件转换到周一
def ntrD_to_trD(oldevent,rtndf):
    ntrDdf = oldevent[~oldevent.index.isin(rtndf.index)]
    #NTR = ntrDdf.reindex(oldevent.index)
    new_df = ntrDdf.reindex(oldevent.index).rolling(2).sum().shift(1)
    NEW = new_df.fillna(0)+oldevent.fillna(0)
    NEW = NEW.mask(NEW>=1,1)
    return NEW

event0 = ntrD_to_trD(event0_df,c1rtndf)

event0.replace(0,np.nan,inplace = True)

AOG_f = AOG_df * event0

mycomm.groupAgg2(AOG_f.fillna(limit = 175,method = 'ffill'),c1rtndf,n =10)
mycomm.show_pic_IC(AOG_f.fillna(limit = 175,method = 'ffill'),c1rtndf,period='M')
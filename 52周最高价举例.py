import pandas as pd
import numpy as np
import os
path = 'F:\\实习生叶恒宇\\超预期研报复现'
os.chdir(path)


import common as mycomm
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 设置中文字体为SimHei
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 导入原始股票池
pool= pd.read_csv('NNN_pool.csv')
pool.set_index('date',inplace = True)
pool.index = pd.to_datetime(pool.index)


###
# 导入价格数据df4
mycomm.connect_toABM()
## 获取收盘价数据 df4
#q = "SELECT TICKER_SYMBOL,TRADE_DATE,CLOSE_PRICE_2 FROM datayes.mkt_equd_adj_af ORDER BY TICKER_SYMBOL,TRADE_DATE;"
#df4 = mycomm.getDTFbySQL(mycomm.connectflag, q, ['TICKER_SYMBOL','TRADE_DATE','CLOSE_PRICE_2'])
#rtn_df_0 = df4.pivot_table(columns='TICKER_SYMBOL',index='TRADE_DATE',values='CLOSE_PRICE_2').reset_index()
#rtn_df = pd.concat([rtn_df_0.iloc[:,:1], rtn_df_0.iloc[:,1:]],axis = 1)
#rtn_df.to_csv('rtn_df.csv')
rtn_df = pd.read_csv('rtn_df.csv')
rtn_df.set_index('TRADE_DATE',inplace = True)
rtn_df.index = pd.to_datetime(rtn_df.index)
rtn_df =  mycomm.cutDF(rtn_df, "20080101", "20240310")
rtn_df = rtn_df.iloc[:,1:]

### 导入中证500因子 zz500 收益率
mycomm.connect_toABM()
q = "SELECT TRADE_DATE,CLOSE_INDEX FROM datayes.mkt_idxd_csi WHERE TICKER_SYMBOL=000905 ORDER BY TRADE_DATE;"
zz500 = mycomm.getDTFbySQL(mycomm.connectflag, q, ['TRADE_DATE','CLOSE_INDEX'])
zz500.set_index('TRADE_DATE',inplace = True)
zz500.index = pd.to_datetime(zz500.index)
zz500 = zz500.pct_change()
zz500 = mycomm.cutDF(zz500, "20090101", "20240310")
# 之后用的是zz500.mean(axis =1)

# 导入收益率文件
next_rtndf = pd.read_csv('next_rtndf.csv')
next_rtndf.set_index('TRADE_DATE',inplace = True)
next_rtndf.index = pd.to_datetime(next_rtndf.index)
next_rtndf =  mycomm.cutDF(next_rtndf, "20090101", "20240310")
c1rtndf = mycomm.cutDF(next_rtndf, "20090101", "20240310")

c2rtndf = mycomm.cutDF(next_rtndf, "20091230", "20240310")

mycomm.connect_toABM()
q ="SELECT TICKER_SYMBOL,ACT_PUBTIME,PUBLISH_DATE,END_DATE,REPORT_TYPE,FISCAL_PERIOD,N_INCOME_ATTR_P FROM datayes.vw_fdmt_is_new WHERE MERGED_FLAG =1 ORDER BY TICKER_SYMBOL,PUBLISH_DATE;"
df6_3 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','N_INCOME_ATTR_P'])
q = "SELECT TICKER_SYMBOL,ACT_PUBTIME,PUBLISH_DATE,END_DATE,REPORT_TYPE,FISCAL_PERIOD,N_INCOME_ATTR_P FROM datayes.fdmt_ee WHERE MERGED_FLAG =1 ORDER BY TICKER_SYMBOL,PUBLISH_DATE;"
df6_2 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','N_INCOME_ATTR_P'])
q = "SELECT TICKER_SYMBOL,ACT_PUBTIME,PUBLISH_DATE,END_DATE,REPORT_TYPE,FISCAL_PERIOD,EXPN_INCAP_LL FROM datayes.fdmt_ef_new WHERE MERGED_FLAG =1 ORDER BY TICKER_SYMBOL,PUBLISH_DATE ;"
df6_1 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','EXPN_INCAP_LL'])
#获取股票前收盘价和最高最低价格信息 st_df3
q = "SELECT trade_date,stock_code,high,low,lclose FROM abmdata.qt_stk_daily;"
st_df3 = mycomm.getDTFbySQL(mycomm.connectflag, q, ['trade_date','stock_code','high','low','lclose'])

mycomm.closeconnect()

df6_3 = mycomm.splitA_P(df6_3)
df6_1 = mycomm.splitA_P(df6_1)
df6_2 = mycomm.splitA_P(df6_2)

# 去除数据中以A开头的股票们
condition = ~df6_1['TICKER_SYMBOL'].str.startswith('A')
df6_1 = df6_1[condition]
condition = ~df6_3['TICKER_SYMBOL'].str.startswith('A')
df6_3 = df6_3[condition]
df5_1 = df6_1.sort_values(['TICKER_SYMBOL','actdate']).iloc[:,[0,-2,-4]]
df5_2 = df6_3.sort_values(['TICKER_SYMBOL','actdate']).iloc[:,[0,-2,-4]]
df5_3 = df6_3.sort_values(['TICKER_SYMBOL','actdate']).iloc[:,[0,-2,-4]]

# 盈余公告披露日期框 df5
df5 = pd.concat([df5_1,df5_2])
df5 = pd.concat([df5,df5_3])

# 将周六周日发生的事件转换到周一
def ntrD_to_trD(oldevent,rtndf):
    ntrDdf = oldevent[~oldevent.index.isin(rtndf.index)]
    #NTR = ntrDdf.reindex(oldevent.index)
    new_df = ntrDdf.reindex(oldevent.index).rolling(2).sum().shift(1)
    NEW = new_df.fillna(0)+oldevent.fillna(0)
    NEW = NEW.mask(NEW>=1,1)
    return NEW

#开始构造因子
event0= ntrD_to_trD(event0_df,c1rtndf)

close_t1 = (event0 *rtn_df).reindex(rtn_df.index)

stt3 = st_df3
st_df3['hrate'] = (st_df3.high - st_df3.lclose)/st_df3.lclose
st_df3['lrate'] = (st_df3.low - st_df3.lclose)/st_df3.lclose


condition1 = ((st_df3.lrate<-0.199) | (st_df3.hrate>0.199))
condition2 = (st_df3.stock_code.str.contains("^30") |st_df3.stock_code.str.contains("^68"))
condition3 =  ((st_df3.lrate<-0.099) | (st_df3.hrate>0.099))

st_df3.drop(st_df3[condition1].index,inplace = True)
st_df3.drop(st_df3[(~condition2)&condition3].index,inplace = True)

st3  = mycomm.getEventCtoR(c1rtndf, st_df3, 'trade_date','stock_code' , 'high')
st3 = st3.shift(-1)
st3 = mycomm.cutDF(st3, "20090101", "20240310")
def ts_max(df, window=10):

    return df.rolling(window,min_periods=1).max()

closet1= close_t1.copy()
closet1.replace(0,np.nan,inplace=True)
PRILAG = (closet1/(ts_max(rtn_df.shift(1),244)))

xg = PRILAG.fillna(method = "ffill")
a = groupAgg2(PRILAG,next_rtndf,10)

show_pic_IC(PRILAG,next_rtndf)
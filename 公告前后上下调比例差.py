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
# 导入收益率文件
next_rtndf =pd.read_csv('next_rtndf.csv')
next_rtndf.set_index (next_rtndf.TRADE_DATE,inplace = True)
next_rtndf = next_rtndf.iloc[:,1:]
c1rtndf = mycomm.cutDF(next_rtndf, "2009-01-01", "2024-03-10")
c1rtndf.replace(0,np.nan,inplace = True)


###
mycomm.connect_toABM()
# 获取上调了净利润预期的原始数据df2
q = "SELECT id,report_id,stock_code,organ_id,current_create_date,np_adjust_mark \
    FROM abmdata.rpt_earnings_adjust ORDER BY current_create_date;"
df2 = mycomm.getDTFbySQL(mycomm.connectflag, q, ['id','report_id','stock_code', \
                                                     'organ_id','current_create_date','np_adjust_mark'])
# 获取研报对应的研究员人数的原始数据df3
q = "SELECT id,report_id,organ_id,author_number from abmdata.rpt_report_author;"
df3 = mycomm.getDTFbySQL(mycomm.connectflag, q, [ 'id','report_id','organ_id','author_number'])

###
mycomm.connect_toABM()
# 获取上市公司披露预报的数据df5_1
q = "SELECT  stock_code,declare_date,is_valid FROM abmdata.fin_performance_forecast ORDER BY stock_code,declare_date;"
df5_1 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['stock_code','declare_date','is_valid'])

# 获取上市公司披露快报的数据df5_2
q =  "SELECT  stock_code,declare_date,is_valid FROM abmdata.fin_performance_express ORDER BY stock_code,declare_date;"
df5_2 = mycomm.getDTFbySQL(mycomm.connectflag, q,  ['stock_code','declare_date','is_valid'])
# 获取上市公司披露财报的数据df5_3
q = "SELECT stock_code,declare_date,np FROM abmdata.fin_income_gen ORDER BY stock_code,declare_date;"
df5_3 =  mycomm.getDTFbySQL(mycomm.connectflag, q,  ['stock_code','declare_date','np'])
df5_3.columns =  ['stock_code','declare_date','is_valid']
# 获取交易日信息
mycomm.connect_toABM()
q = "SELECT distinct trade_date FROM abmdata.qt_stk_daily;"
trade_date =mycomm.getDTFbySQL(mycomm.connectflag, q, ['trade_date'])

# 先计算各个股票的UDPCT
df2_3 = pd.merge(df2,df3,on ='report_id')
df23 = df2_3.fillna(0)
df23.np_adjust_mark = df23.np_adjust_mark.mask(df23.np_adjust_mark == 1,0)
df23.np_adjust_mark = df23.np_adjust_mark.mask(df23.np_adjust_mark == 2,1)

df23.np_adjust_mark = df23.np_adjust_mark.mask(df23.np_adjust_mark == 3,-1)
df23 = df23.iloc[:,[1,2,4,5,8]]
df23['product1'] = df23.np_adjust_mark * df23.author_number
event1 = df23.groupby(['current_create_date','stock_code'])['product1'].sum()
event1df = event1.unstack()

df23.np_adjust_mark = df23.np_adjust_mark.mask(df23.np_adjust_mark == -1,1)
df23['product2'] = df23.np_adjust_mark * df23.author_number
event2 =  df23.groupby(['current_create_date','stock_code'])['product2'].sum()
event2df = event2.unstack()

#zhengfu = (event1df/event2df)/(abs((event1df/event2df)))

event =event1df/event2df +event2df/10000
event = event.mask(event == np.inf,0)
event = event.mask(event == -np.inf,0)

#event  = event1df.mask(event1df>0,1)*event2df + event1df.mask(event1df<0,-1)*event2df
#event =event1df
#event = np.log(event2df+1)


event = event.reindex(
    index = pd.date_range(
        next_rtndf.index[0],next_rtndf.index[-1]
    ),
    columns = next_rtndf.columns
)
event = event.fillna(0)
# 再设置五天内覆盖大于三人的指标
event_2 = df23.groupby(['current_create_date','stock_code'])['product2'].sum()
event2_df = event_2.unstack()

event2_df = event2_df.reindex(
    index = pd.date_range(
        next_rtndf.index[0],next_rtndf.index[-1]
    ),
    columns = next_rtndf.columns
)
event2_df = event2_df.mask(event2_df<3,0)
event2_df = event2_df.mask(event2_df>=3,1)
event2_df = event2_df.fillna(0)


# 需要盈余公告来筛选
df5 = pd.concat([df5_1,df5_2])
df5 = pd.concat([df5,df5_3])
event0_df = mycomm.getEventCtoR(c1rtndf, df5,  'declare_date','stock_code', 'is_valid')


event22_df =(event0_df * event2_df)
event22_df = event22_df.shift(4)

# 最后得到UDPCT(暂定)
UDPCT = (event.shift(4))* event22_df
UDPCT = mycomm.cutDF(UDPCT, "20090101", "20240310")
UDPCT.replace(0,np.nan,inplace = True)

xg = UDPCT.copy()
xg = xg.fillna(method = 'ffill',limit = 90)

aaa = mycomm.groupAgg2(xg,next_rtndf,10)
show_pic_IC(xg,next_rtndf,period='M')
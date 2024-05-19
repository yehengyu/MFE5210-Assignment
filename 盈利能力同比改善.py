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

# 导入收益率文件
next_rtndf = pd.read_csv('next_rtndf.csv')
next_rtndf.set_index('TRADE_DATE',inplace = True)
next_rtndf.index = pd.to_datetime(next_rtndf.index)
next_rtndf =  mycomm.cutDF(next_rtndf, "20090101", "20240310")
c1rtndf = mycomm.cutDF(next_rtndf, "20090101", "20240310")
c1rtndf.replace(0,np.nan,inplace = True)
c2rtndf = mycomm.cutDF(next_rtndf, "20091230", "20240310")
c2rtndf.replace(0,np.nan,inplace = True)

BS = pd.read_csv('df_vw_fdmt_bs_new.csv')
IS = pd.read_csv('df_vw_fdmt_is_new.csv')

# ## 获取合并报表的归母净利润，归母投资者权益数据df1，df2
df2 = BS[['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','T_EQUITY_ATTR_P','MERGED_FLAG']]
df2 =  df2[df2.MERGED_FLAG==1]

df1 = IS[['TICKER_SYMBOL','ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','N_INCOME_ATTR_P','MERGED_FLAG']]
df1 = df1[df1.MERGED_FLAG ==1]

df1.PUBLISH_DATE = pd.to_datetime(df1.PUBLISH_DATE)
df1.END_DATE = pd.to_datetime(df1.END_DATE)

df2.PUBLISH_DATE = pd.to_datetime(df2.PUBLISH_DATE)
df2.END_DATE = pd.to_datetime(df2.END_DATE)
df1 = df1.iloc[:,[0,1,2,3,4,5,6]]
df2 = df2.iloc[:,[0,1,2,3,4,5,6]]

df1mul = df1.copy()
df2mul = df2.copy()

DF1 = df1mul.drop_duplicates(subset=['ACT_PUBTIME','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','N_INCOME_ATTR_P'])
DF2 = df2mul.drop_duplicates(subset=['TICKER_SYMBOL','PUBLISH_DATE','END_DATE','REPORT_TYPE','FISCAL_PERIOD','T_EQUITY_ATTR_P'])

DF1.TICKER_SYMBOL = mycomm.fill_stkcd(DF1.TICKER_SYMBOL)
DF2.TICKER_SYMBOL = mycomm.fill_stkcd(DF2.TICKER_SYMBOL)
DF11 = DF1
DF22 = DF2

#DF11.set_index(['TICKER_SYMBOL','PUBLISH_DATE'], inplace = True)
DF11 = DF11.sort_values(['TICKER_SYMBOL','PUBLISH_DATE'])
DF11.END_DATE = DF11.END_DATE.astype(str)
DF11[['REPORT_YEAR','tmp1','tmp2']] = DF11.END_DATE.str.split("-",expand = True)
DF11 = DF11.drop(columns = ["tmp1","tmp2"])
DF11.FISCAL_PERIOD = DF11.FISCAL_PERIOD.astype(str)

DF22 = DF22.sort_values(['TICKER_SYMBOL','PUBLISH_DATE'])
DF22.END_DATE = DF22.END_DATE.astype(str)
DF22[['REPORT_YEAR','tmp1','tmp2']] = DF22.END_DATE.str.split("-",expand = True)
DF22 = DF22.drop(columns = ["tmp1","tmp2"])
DF22.FISCAL_PERIOD = DF22.FISCAL_PERIOD.astype(str)


# condition = ((df['REPORT_TYPE']=="S1") & (df["FISCAL_PERIOD"]==6))

def fetch_dayvalue(fac_date, data_df, value_name, rep_year="2022", rep_type='Q1', fic_per='3', ind1="TICKER_SYMBOL",
                   ind2="PUBLISH_DATE"):
    # fac_date表明因子生成时的日期，如"2024-03-01"，data_df指代原有的数据帧，它最好是已经整理过的，有股票代码，publish_date,REPORT_YEAR,REPORT_TYPE,FISCAL_PERIOD，且按序排列的)
    df = data_df[
        (data_df.REPORT_TYPE == rep_type) & (data_df.FISCAL_PERIOD == fic_per) & (data_df.REPORT_YEAR == rep_year)]

    # 获取delta一年的数据，只需回看470天，保证数据的时效性
    df = df[((df[ind2] - np.datetime64(fac_date)) <= np.timedelta64(0, 'D')) & (
                (np.datetime64(fac_date) - df[ind2]) < np.timedelta64(470, 'D'))]

    # 获取最新数据，即与fac_date时间相隔最近的数据
    def find_near(df):
        return df.loc[(np.abs(df.PUBLISH_DATE - np.datetime64(fac_date))).idxmin()]

    df = df.groupby('TICKER_SYMBOL').apply(find_near)

    return (df[value_name].astype(float))


def merge_dayvalue2(data_df1, value_name1, data_df2, value_name2, rtndf, i=0):
    # rtndf只提供数据框index和columns，不参与计算,i来判断：数据计算的是当年还是去年，0则当年，1则去年
    temp = pd.DataFrame(columns=rtndf.columns, index=rtndf.index)

    def innerFunc(row):

        fanhui = row

        if ((row.name >= pd.Timestamp(year=row.name.year, month=1, day=1)) &
                (row.name <= pd.Timestamp(year=row.name.year, month=3, day=30))):
            a = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 1), "A", "12").reindex(
                rtndf.columns) - \
                fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 1), "Q3", "9").reindex(
                    rtndf.columns)

            b = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i - 1), "A", "12").reindex(
                rtndf.columns)

            c = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 2), "A", "12").reindex(
                rtndf.columns) - \
                fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 2), "Q3", "9").reindex(
                    rtndf.columns)

            d = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i - 2), "A", "12").reindex(
                rtndf.columns)

            fanhui = (a / b - c / d)

        elif ((row.name >= pd.Timestamp(year=row.name.year, month=4, day=1)) &
              (row.name <= pd.Timestamp(year=row.name.year, month=4, day=30))):
            a = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i), "Q1", "3").reindex(
                rtndf.columns)
            b = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i), "Q1", "3").reindex(
                rtndf.columns)
            c = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 1), "Q1", "3").reindex(
                rtndf.columns)
            d = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i - 1), "Q1", "3").reindex(
                rtndf.columns)
            fanhui = a / b - c / d


        elif ((row.name >= pd.Timestamp(year=row.name.year, month=7, day=1)) &
              (row.name <= pd.Timestamp(year=row.name.year, month=8, day=30))):
            a = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i), "S1", "6").reindex(
                rtndf.columns) - \
                fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i), "Q1", "3").reindex(
                    rtndf.columns)
            b = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i), "S1", "6").reindex(
                rtndf.columns)
            c = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 1), "S1", "6").reindex(
                rtndf.columns) - \
                fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 1), "Q1", "3").reindex(
                    rtndf.columns)
            d = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i - 1), "S1", "6").reindex(
                rtndf.columns)
            fanhui = a / b - c / d


        elif ((row.name >= pd.Timestamp(year=row.name.year, month=10, day=1)) &
              (row.name <= pd.Timestamp(year=row.name.year, month=10, day=31))):
            tempA1 = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i), "Q3", "9").reindex(
                rtndf.columns) - fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year), "S1", "6").reindex(
                rtndf.columns)
            tempB1 = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i), "Q3", "3").reindex(
                rtndf.columns)
            a = tempA1.combine_first(tempB1)
            tempA2 = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i), "Q3", "9").reindex(
                rtndf.columns)
            b = tempA2

            tempA1 = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 1), "Q3", "9").reindex(
                rtndf.columns) - fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year), "S1", "6").reindex(
                rtndf.columns)
            tempB1 = fetch_dayvalue(row.name, data_df1, value_name1, str(row.name.year - i - 1), "Q3", "3").reindex(
                rtndf.columns)
            c = tempA1.combine_first(tempB1)
            tempA2 = fetch_dayvalue(row.name, data_df2, value_name2, str(row.name.year - i - 1), "Q3", "9").reindex(
                rtndf.columns)
            d = tempA2

            fanhui = a / b - c / d

        return fanhui

        # 注意到年报本来就只会报导去年的数据

    temp = temp.apply(innerFunc, axis=1)

    # temp = temp1.combine_first(temp2)
    # temp = temp.fillna(0) + temp3.fillna(0) +temp4.fillna(0)
    # temp.replace(0,np.nan,inplace = True)
    return temp

bbbb = merge_dayvalue2(DF11,"N_INCOME_ATTR_P",DF22,"T_EQUITY_ATTR_P",c1rtndf,i = 0)
bbbb.replace(np.inf,np.nan,inplace = True)
bbbb.replace(-np.inf,np.nan,inplace = True)

mycomm.groupAgg2(bbbb.fillna(method = 'ffill',limit = 90),c2rtndf,n =10)
mycomm.show_pic_IC(bbbb.fillna(method = 'ffill',limit = 100),c2rtndf,period='M')


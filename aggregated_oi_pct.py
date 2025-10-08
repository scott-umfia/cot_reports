import datetime
import MySQLdb
import sys
import os
import numpy as np
import pandas as pd
from pandas import Series, DataFrame
import matplotlib as mpl
from matplotlib.ticker import NullFormatter
#from scipy import stats
from math import sqrt

mpl.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib.cbook as cbook
import matplotlib.ticker as ticker

from datetime import datetime,date

PYTHON_FILE_NAME = os.path.basename(__file__)

# -----------------------------------------------------------------
def LoadSqlData(sql, conn):
   cursor = conn.cursor()
   cursor.execute(sql)
   rows = cursor.fetchall()
   cols = zip(*cursor.description)[0]
   arr = np.array(rows)
   data = DataFrame(arr, columns = cols)

   return data

# -----------------------------------------------------------------


# make mysql connection
conn_colo = MySQLdb.connect(host = 'HOST', user = 'USER', passwd = 'PASSWORD', db = 'DB_TABLE', port = 3306)

# create list of symbols
symbols = ['BO','C-','CC','CL','CT','FC','GC','HO','KC','LC','LH','NG','S-','SB','SI','SM','W-']

# array of DataFrames
data_array = []

for symbol in symbols:


    # create dataframe of effective dates of given account and refsymbol
    sql = "select a.As_of_Date_Form_MM_DD_YYYY as mdy, (a.Prod_Merc_Positions_Long_All+a.Prod_Merc_Positions_Short_All)/Open_Interest_All as Prod_Merc_Pct_Of_Tot_OI,(a.Swap_Positions_Long_All+a.Swap__Positions_Short_All-0*a.Swap__Positions_Spread_All)/a.Open_Interest_All as Swap_Pct_Of_Tot_OI,(a.M_Money_Positions_Long_All+a.M_Money_Positions_Short_All-0*a.Swap__Positions_Spread_All)/a.Open_Interest_All as MMoney_Pct_Of_Tot_OI, a.Conc_Net_LE_4_TDR_Long_All+a.Conc_Net_LE_4_TDR_Short_All as Conc_Net_4_ALL,a.Conc_Net_LE_8_TDR_Long_All+a.Conc_Net_LE_8_TDR_Short_All as Conc_Net_8_ALL from optiondata.cot_disaggregated_fut_opt as a where a.cftc_contract_market_code = (select cftc_code from futures_info where futures_root='" + symbol + "')  order by a.As_of_Date_Form_MM_DD_YYYY"
    df1 = LoadSqlData(sql, conn_colo)
    df1.index = df1['mdy']
    data_array.append(df1)

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(df1.index, df1['Prod_Merc_Pct_Of_Tot_OI'])
    ax.plot(df1.index, df1['Swap_Pct_Of_Tot_OI'])
    ax.plot(df1.index, df1['MMoney_Pct_Of_Tot_OI'])
    ax.set_title(symbol + " ProdMerc vs Swap vs MMoney Pct OI")
    ax.legend( ('ProdMerc', 'Swap', 'MMoney'), 'upper left', shadow=True )
    fig.autofmt_xdate()
    ax.grid(True)
    plt.savefig("prod_swap_mmoney_oi_"+symbol+".png")

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(df1.index, df1['Conc_Net_4_ALL'])
    ax.plot(df1.index, df1['Conc_Net_8_ALL'])
    ax.set_title(symbol+" Top 4 and 8 Reported Positions OI %")
    ax.legend( ('Top 4', 'Top 8'), 'upper left', shadow=True )
    fig.autofmt_xdate()
    ax.grid(True)
    plt.savefig("oi_concentration_"+symbol+".png")

    # output df to .csv 
    df1.to_csv('data_' + symbol + '.csv');
    print symbol,len(df1)



# find min and max mdy in array of DataFrames
min_dte = data_array[0].index[0]
max_dte = data_array[0].index[-1]

for df in data_array:
    if df.index[0] < min_dte:
        min_dte = df.index[0]
    if df.index[-1] > max_dte:
        max_dte = df.index[-1]

# construct DataFrames with range of min/max mdy
sql_dates = 'select distinct(As_of_Date_Form_MM_DD_YYYY) as mdy from optiondata.cot_disaggregated_fut_opt where As_of_Date_Form_MM_DD_YYYY>= "' + str(min_dte) + '" and As_of_Date_Form_MM_DD_YYYY<= "' + str(max_dte) + '" order by As_of_Date_Form_MM_DD_YYYY'
master_data = LoadSqlData(sql_dates, conn_colo)
master_data.index = master_data['mdy']
master_data['prod_merc_pct_OI'] = Series([0]*len(master_data), index=master_data.index)
master_data['swap_pct_OI'] = Series([0]*len(master_data), index=master_data.index)
master_data['mmoney_pct_OI'] = Series([0]*len(master_data), index=master_data.index)
master_data['conc_net_4'] = Series([0]*len(master_data), index=master_data.index)
master_data['conc_net_8'] = Series([0]*len(master_data), index=master_data.index)

del[master_data['mdy']]


# loop through array of DataFrames and merge each data frame to master
# add OI and concentration ratios to a master column
for df in data_array:

    # add elements to master columns
    master_data = DataFrame.join(master_data, df, how='left').fillna(value=0)
    master_data['prod_merc_pct_OI'] = master_data['prod_merc_pct_OI'] + master_data['Prod_Merc_Pct_Of_Tot_OI']
    master_data['swap_pct_OI'] = master_data['swap_pct_OI'] + master_data['Swap_Pct_Of_Tot_OI']
    master_data['mmoney_pct_OI'] = master_data['mmoney_pct_OI'] + master_data['MMoney_Pct_Of_Tot_OI']
    master_data['conc_net_4'] = master_data['conc_net_4'] + master_data['Conc_Net_4_ALL']
    master_data['conc_net_8'] = master_data['conc_net_8'] + master_data['Conc_Net_8_ALL']

    # delete columns to avoid duplicate names
    del[master_data['Prod_Merc_Pct_Of_Tot_OI']]
    del[master_data['Swap_Pct_Of_Tot_OI']]
    del[master_data['MMoney_Pct_Of_Tot_OI']]
    del[master_data['Conc_Net_4_ALL']]
    del[master_data['Conc_Net_8_ALL']]
    del[master_data['mdy']]

# fill in missing data
master_data['prod_merc_pct_OI'] = master_data['prod_merc_pct_OI'].replace(0, method='ffill')
master_data['swap_pct_OI']  = master_data['swap_pct_OI'].replace(0, method='ffill')
master_data['mmoney_pct_OI'] = master_data['mmoney_pct_OI'].replace(0, method='ffill')
master_data['conc_net_4'] = master_data['conc_net_4'].replace(0, method='ffill')
master_data['conc_net_8'] = master_data['conc_net_8'].replace(0, method='ffill')

master_data['prod_merc_pct_OI'] = master_data['prod_merc_pct_OI'] / float(len(data_array))
master_data['swap_pct_OI']  = master_data['swap_pct_OI'] / float(len(data_array))
master_data['mmoney_pct_OI'] = master_data['mmoney_pct_OI'] / float(len(data_array))
master_data['conc_net_4'] = master_data['conc_net_4'] / float(len(data_array))
master_data['conc_net_8'] = master_data['conc_net_8'] / float(len(data_array))


fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(master_data.index, master_data['prod_merc_pct_OI'])
ax.plot(master_data.index, master_data['swap_pct_OI'])
ax.plot(master_data.index, master_data['mmoney_pct_OI'])
ax.set_title("ProdMerc vs Swap vs MMoney Pct OI")
ax.legend( ('ProdMerc', 'Swap', 'MMoney'), 'upper left', shadow=True )
fig.autofmt_xdate()
ax.grid(True)
plt.savefig("prod_swap_mmoney_oi.png")

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(master_data.index, master_data['conc_net_4'])
ax.plot(master_data.index, master_data['conc_net_8'])
ax.set_title("Top 4 and 8 Reported Positions OI %")
ax.legend( ('Top 4', 'Top 8'), 'upper left', shadow=True )
fig.autofmt_xdate()
ax.grid(True)
plt.savefig("oi_concentration.png")

master_data.to_csv('data_aggregated.csv')


plt.show()



print "Success"

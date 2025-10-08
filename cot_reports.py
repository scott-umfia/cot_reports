import datetime
import MySQLdb
import sys
import os
import numpy as np
import matplotlib as mpl
from matplotlib.ticker import NullFormatter
from scipy.stats import percentileofscore

# dont use graphical interface
#mpl.use('Agg')

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib.cbook as cbook
import matplotlib.ticker as ticker

PYTHON_FILE_NAME = os.path.basename(__file__)

# -----------------------------------------------------------------
# Configuration Settings
dbHost     = '127.0.0.1'
dbPort     = 3306
#dbPort     = 2048
dbSchema   = 'optiondata'
dbUser     = 'root'
dbPassword = 'sparta'
images = []

# -----------------------------------------------------------------
# Class used to hold data
class COTdata:
   mdy 		= ""
   hp		= ""
   ac		= ""
   ohp		= ""
   aco		= ""
   
# -----------------------------------------------------------------


# Log job start
commandLine = ""
for arg in sys.argv:
   commandLine = commandLine + arg + " "
commandLine = commandLine.strip()
message = ""

# Parse input arguments
usageErrorMsg = "Usage: python " + PYTHON_FILE_NAME + " [commodity_code startdate enddate]\n" 
if len(sys.argv) == 4:
   commodity_code = sys.argv[1]
   startdate = sys.argv[2]
   enddate = sys.argv[3]
else:
   sys.exit(usageErrorMsg)


# Get backtest info from db
conn = MySQLdb.connect(host = dbHost, user = dbUser, passwd = dbPassword, db = dbSchema, port = dbPort)
cursor = conn.cursor ()

sql = "select a.Report_Date_as_MM_DD_YYYY, \
    round((b.comm_positions_short_all - b.comm_positions_long_all)/(b.comm_positions_short_all + b.comm_positions_long_all),4) as HP, \
    round((b.noncomm_positions_long_all - b.noncomm_positions_short_all)/(b.noncomm_positions_short_all + b.noncomm_positions_long_all),4) as AC, \
    round(((a.comm_positions_short_all-b.comm_positions_short_all) - (a.comm_positions_long_all-b.comm_positions_long_all))/((a.comm_positions_short_all-b.comm_positions_short_all) + (a.comm_positions_long_all-b.comm_positions_long_all)),4) as OHP, \
 round(((((a.open_interest_all - b.open_interest_all) - (a.comm_positions_long_all - b.comm_positions_long_all) -  (a.nonrept_positions_long_all  - b.nonrept_positions_long_all)) - ((a.open_interest_all - b.open_interest_all) - (a.comm_positions_short_all - b.comm_positions_short_all) - (a.nonrept_positions_short_all - b.nonrept_positions_short_all))))/((((a.open_interest_all - b.open_interest_all) - (a.comm_positions_long_all - b.comm_positions_long_all) - (a.nonrept_positions_long_all - b.nonrept_positions_long_all)) + ((a.open_interest_all - b.open_interest_all) - (a.comm_positions_short_all - b.comm_positions_short_all) - (a.nonrept_positions_short_all - b.nonrept_positions_short_all)))),4) as ACO \
from optiondata.cot_legacy_fut_opt as a inner join optiondata.cot_legacy_fut as b on  a.Report_Date_as_MM_DD_YYYY = b.Report_Date_as_MM_DD_YYYY and a.cftc_contract_market_code=b.cftc_contract_market_code where a.cftc_contract_market_code = '" + commodity_code + "' and a.Report_Date_as_MM_DD_YYYY>='" + startdate + "' and a.Report_Date_as_MM_DD_YYYY<='" + enddate + "' order by a.Report_Date_as_MM_DD_YYYY"

cursor.execute(sql)
symbolRows = cursor.fetchall ()

cot = []
for row in symbolRows:
   data = COTdata()
   data.mdy = row[0]
   data.hp = row[1]
   data.ac = row[2]
   data.ohp = row[3]
   data.aco = row[4]
   cot.append(data)


# Write data to text file

# plot COT reports
x = []
hp = []
ac = []
ohp = []
aco = []

for item in cot:
   x.append(item.mdy)
   hp.append(item.hp)
   ac.append(item.ac)
   ohp.append(item.ohp)
   aco.append(item.aco)

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(x, hp)
ax.plot(x, ac)
ax.set_title("HP vs AC : " + commodity_code)
ax.legend( ('HP', 'AC'), 'upper left', shadow=True )
fig.autofmt_xdate()
ax.grid(True)

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(x, ohp)
ax.plot(x, aco)
ax.set_title("OHP vs ACO : " + commodity_code)
ax.legend( ('OHP','ACO'), 'upper left', shadow=True )
fig.autofmt_xdate()
ax.grid(True)

plt.show()


# add scatter plot of hp vs ac
#fig = plt.figure(figsize=(8, 6))
#ax = fig.add_subplot(111, axisbg='#FFFFCC')
#A = np.vstack([imp_less_realized_vol, np.ones(len(imp_less_realized_vol))]).T
#m,c = np.linalg.lstsq(A,pnl_change_62)[0]
#ax.plot(imp_less_realized_vol, pnl_change_62, 'o',markersize=5)
#fitted_line=[]
#for x_val in imp_less_realized_vol:
#    fitted_line.append(m*x_val + c)
#ax.plot(imp_less_realized_vol, fitted_line, 'r', label='Fitted line')
#ax.set_xlim(np.min(imp_less_realized_vol)-.05*abs(np.min(imp_less_realized_vol)), np.max(imp_less_realized_vol)+.05*abs(np.max(imp_less_realized_vol)))
#ax.set_ylim(np.min(pnl_change_62)-0.05*abs(np.min(pnl_change_62)), np.max(pnl_change_62)+.05*abs(np.max(pnl_change_62)))
#ax.set_title('2mo Imp-Realized Vol vs Pnl : m= ' + str(round(m,2)) + ' b= ' + str(round(c,4)) + '\nrun_number=' + str(run_number))
#ax.grid(True)


# add histogram of port_returns
#fig =plt.figure()
#ax = fig.add_subplot(111)
#n, bins, patches = ax.hist(r.port_return, 50, normed=1, facecolor='green', alpha=0.75)
#bincenters = 0.5*(bins[1:]+bins[:-1])
#mu = np.mean(r.port_return)
#sigma = np.std(r.port_return)
#y = mlab.normpdf( bincenters, mu, sigma)
#l = ax.plot(bincenters, y, 'r--', linewidth=1)
#min_ret = np.min(r.port_return)
#max_ret = np.max(r.port_return)
#ax.set_xlabel('Port Return')
#ax.set_ylabel('Probability')
#ax.set_title('Distribution of Daily Returns : '+ comment + ' run_number=' + str(run_number) + '\nMin/Max Returns : ' + str(round(100*min_ret,2)) + '% / ' + str(round(100*max_ret,2))+'%')
#ax.grid(True)





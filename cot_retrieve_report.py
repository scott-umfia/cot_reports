import datetime
#import MySQLdb
import sys
import os
#import utilities
import matplotlib as mpl

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import matplotlib.cbook as cbook
import matplotlib.ticker as ticker

import urllib

PYTHON_FILE_NAME = os.path.basename(__file__)

# -----------------------------------------------------------------
# Configuration Settings
dbHost     = '127.0.0.1'
#dbPort_local     = 3306
dbPort_colo     = 2048
dbSchema   = 'optiondata'
dbUser     = 'root'
dbPassword = 'sparta'
# -----------------------------------------------------------------


# Log job start
commandLine = ""
for arg in sys.argv:
   commandLine = commandLine + arg + " "
commandLine = commandLine.strip()
message = ""
#startTime = utilities.LogJobStart('cot_retrieve_report', commandLine, message)


# Parse input arguments
usageErrorMsg = "Usage: python " + PYTHON_FILE_NAME + " [F=fut only file, FO=fut&opt file]\n" 
if len(sys.argv) == 2:
   file_type = str(sys.argv[1]).strip()
else:
   sys.exit(usageErrorMsg)


# make connections to colo and localserver
#conn_colo = MySQLdb.connect(host = dbHost, user = dbUser, passwd = dbPassword, db = dbSchema, port = dbPort_colo)
#cursor_colo = conn_colo.cursor ()

#conn_local = MySQLdb.connect(host = dbHost, user = dbUser, passwd = dbPassword, db = dbSchema, port = dbPort_local)
#cursor_local = conn_local.cursor ()

url = ''
filename = ''

if file_type == 'F':
   # futures only file
   url = 'http://www.cftc.gov/dea/newcot/deafut.txt'
   filename = '/home/ubuntu/ebs/projects/python_code/cot_reports/cot_data/cot_fut.txt'
   sql_start = "replace into historical.cot_legacy_fut values ("
elif file_type == 'FO':
   # futures and options file
   url = 'http://www.cftc.gov/dea/newcot/deacom.txt'
   filename = '/home/ubuntu/ebs/projects/python_code/cot_reports/cot_data/cot_futopt.txt'
   sql_start = "replace into historical.cot_legacy_fut_opt values ("
elif file_type == 'CIT':
   # COT supplement file
   url = 'http://www.cftc.gov/dea/newcot/deacit.txt'
   filename = '/home/ubuntu/ebs/projects/python_code/cot_reports/cot_data/cot_cit.txt'
   sql_start = "replace into historical.cot_index_supplement values ("
elif file_type == 'DIS_F':
   # disaggregated futures file
   url = 'http://www.cftc.gov/dea/newcot/f_disagg.txt'
   filename = '/home/ubuntu/ebs/projects/python_code/cot_reports/cot_data/cot_dis_fut.txt'
   sql_start = "replace into historical.cot_disaggregated_fut values ("
elif file_type == 'DIS_FO':
   #    # disaggregated futures and options file
   url = 'http://www.cftc.gov/dea/newcot/c_disagg.txt'
   filename = '/home/ubuntu/ebs/projects/python_code/cot_reports/cot_data/cot_dis_futopt.txt'
   sql_start = "replace into historical.cot_disaggregated_fut_opt values ("



urllib.urlretrieve(url, filename)
data = mlab.csv2rec(filename ,delimiter=',')

for row in data:
   sql = sql_start
   for elem in row:
      label = str(elem).strip()
      label = label.replace("'","")
      sql = sql + "'" + label + "',"
   sql = sql[:-1] + ")"
   print (sql)
   #	cursor_colo.execute(sql)
   #	conn_colo.commit()


if False:
   for row in data:
      if len(row)==129:
         sql = sql_start
      
         for i in range(126):
            label = str(row[i]).strip()
            label = label.replace("'","")
            if i < 125:
               sql = sql + "'" + label + "',"
            else:
               sql = sql + "'" + label + "')"
      
         print (str(row[2]).strip(),str(row[0]).strip())
      #print sql
   #	cursor_local.execute(sql)
   #	conn_local.commit()
   #	cursor_colo.execute(sql)
   #	conn_colo.commit()
      elif len(row)==54:
         sql = sql_start
         for i in range(54):
            label = str(row[i]).strip()
            label = label.replace("'","")
            if i < 53:
               sql = sql + "'" + label + "',"
            else:
               sql = sql + "'" + label + "')"
         print (str(row[2]).strip(),str(row[0]).strip())
      #print sql
      #cursor_local.execute(sql)
      #conn_local.commit()
   #	cursor_colo.execute(sql)
   #	conn_colo.commit()
      elif len(row)==191:
         sql = sql_start
         for i in range(191):
            label = str(row[i]).strip()
            label = label.replace("'","")
            if i < 190:
               sql = sql + "'" + label + "',"
            else:
               sql = sql + "'" + label + "')"
         print (str(row[2]).strip(),str(row[0]).strip())
      #print sql
      #cursor_local.execute(sql)
      #conn_local.commit()
   #	cursor_colo.execute(sql)
   #	conn_colo.commit()

#conn_colo.close()
#conn_local.close()

# ----------------------

# Log job completion
#utilities.LogJobEnd('cot_retrieve_report', startTime, 'Success', '')

# ----------------------


#!/usr/bin/env python
# coding: utf-8

# In[1]:
import warnings
warnings.filterwarnings("ignore")

from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
display(HTML("<style>.container { height:100% !important; }</style>"))
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

import sys, gc, itertools, copy 
import pandas as pd, numpy as np, math, networkx as nx
from datetime import datetime, date, timedelta


# pd.options.display.float_format = '${:,.0f}'.format
pd.options.display.float_format = '{:,.2f}'.format

# sys.path.insert(0, '/home/ec2-user/SageMaker/ClientConnect/Functions')
# import data_functions as d
# import bg_aro_functions as b

env = 'prod'

brand = 'nm'
wbrand='NM'

today = date.today()


window = 56
end_day_TY = date.today()
bgn_day_TY = date.today() - timedelta(days=window)
end_day_LY = date.today() - timedelta(days=365 - 1) # 1 day is for Feb 29, 2020, to align week day to week day
bgn_day_LY = date.today() - timedelta(days=365 - 1+ window)

# bgn_day_string = '2019-01-01'
# end_day_string = '2019-11-30'
# bgn_day = datetime.strptime(bgn_day_string,'%Y-%m-%d')
# end_day = datetime.strptime(end_day_string,'%Y-%m-%d')

parameters_TY = [bgn_day_TY, end_day_TY]
parameters_LY = [bgn_day_LY, end_day_LY]

now = datetime.now()
today_name = now.strftime("%A")[0:3]
# print(today_name)


abandon_cart_run_interval = 7 # in days

# Date parameter

idx = (today.weekday() + 1) % 7
last_sat = today - timedelta(7+idx-6)
bgn_day = last_sat-timedelta(days=366)
end_day = last_sat
parameters_T = ['MKTSAND.sk_{}_revenue_TY'.format(wbrand), bgn_day, end_day, wbrand]


Twoyr_day = bgn_day - timedelta(days=365)

This_Fiscal_Year = 2020
Last_Fiscal_Year = str(This_Fiscal_Year-1)
Fiscal_Year_start = str(This_Fiscal_Year)+'00'
# print('Fiscal Year Start: ', Fiscal_Year_start[0:4])
parameters_rolling = [wbrand,Fiscal_Year_start]



sunday = today + timedelta( (6-today.weekday()) % 7)
sunday2 = (sunday + timedelta(days = 6))
track_date = str(today.strftime('%m%d%Y'))
ts_creation = pd.Timestamp(today)
ts_creation = ts_creation.now()


abandon_cart_last_run_date = today - timedelta(abandon_cart_run_interval)


# notebook 1 paramters


# Notebook 2 parameter
# note, this is for browse data
begin_day = today-timedelta(days=60)
parameters_B = [begin_day,today,brand]

# parameter for collecting digital customers
parameters_digital = [bgn_day,end_day,wbrand]

# paramters_notebook_5

begin_day = today-timedelta(days=365)
a_date = today.strftime("%m.%d.%Y")
a_date = str(a_date[0:8])
date_lw = today - timedelta(days=7)
date_lw = date_lw.strftime("%m.%d.%Y")
date_lw = str(date_lw[0:8])


## file information
bucket_nameX = 'nmg-analytics-ds-prod'
Parameter_dir =  'ds/{0}/ClientConnect/Parameters/{1}/'.format(env,brand)
# Model_dir = 'ds/{0}/ClientConnect/model/{1}/{2}/'.format(env,brand,today)
# Prod_dir = 'ds/{0}/ClientConnect/data/{1}/{2}/'.format(env,brand,today)
Model_dir = 'ds/{0}/ClientConnect/model/{1}/'.format(env,brand)
Prod_dir = 'ds/{0}/ClientConnect/data/{1}/'.format(env,brand)
Abandon_cart_icc_dir = 'from_ICC/abandon_cart/'

Dev_dir_mask = 'Users/nmsk408_sohel/'
# print('Dev_dir_mask: ', Dev_dir_mask)
save_dir_nameX = Dev_dir_mask + Prod_dir
# print('save_dir_nameX: ', save_dir_nameX)
read_dir_nameX = save_dir_nameX
#read_dir_nameX = Dev_dir_mask + Prod_dir
# print('read_dir_nameX: ', read_dir_nameX)
network_dir = 'Users/nmsk408_sohel/Network/'
# print('network_dir :', network_dir)
time_feature_dir = 'Users/nmsk408_sohel/time_feature_dir/'
product_feature_dir = 'Users/nmsk408_sohel/product_feature_dir/'

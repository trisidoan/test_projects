#!/usr/bin/env python
# coding: utf-8

### This is a data input and output utility script

from Functions import common_header as h
from Functions import tools as t

import pandas as pd, numpy as np, math


# In[8]:
def sf_connection():
    import sys
    p = '/home/ec2-user/SageMaker/Repos/data-science'
    if p not in sys.path:
        sys.path.append(p)
    from nm_data_sci.common import ssm_cnx
    sf_cnx = ssm_cnx.get_snowflake_connection()
    return sf_cnx


   
def select_transaction_data(parameters):
    # curr_cmd_id, customerid, cmd_id, ...
    # parameters = [from_date, to_date, chain]
    ## NM Store only by  mktsand.location_channel_v lv.channel = 'NM_store'
    

    sql_query = """ 
select L.* from
        (select curr_cmd_id as curr_customer_id,trans_date,brand_name,class_name,pim_id,Location_dim_key,department_id,department_name,
        division_id, itm_associate_dim_key,itm_associate2_dim_key,channel, hdr_tran_no, sum(trans_amount) total_spent
        from
        (
        select distinct s.curr_cmd_id, s.hdr_business_date as trans_date, s.brand_name, p.class_name, p.division_name, s.division_id,
       p.department_id, p.department_name, s.pim_id, s.Location_dim_key, s.channel, s.itm_associate_dim_key,
       s.itm_associate2_dim_key, s.hdr_tran_no, (s.itm_unit_net_purchase_amt*s.itm_qty) as trans_amount
        from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
        inner join nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
        inner join pdwdm.product_dim p on (s.pim_id = p.pim_id)
        inner join pdwdm.location_dim l on (s.location_dim_key = l.location_dim_key)
        where (
                (s.hdr_business_date between to_date('{0}') and to_date('{1}' ))
                and (s.brand = '{2}')
                and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                and (s.itm_item_status != 'V')
                and (s.curr_cmd_id != '0')
                and (s.division_id != 27)
                )
             )X
        group by curr_cmd_id,trans_date,brand_name,class_name,pim_id,Location_dim_key,department_id,department_name,
        division_id,itm_associate_dim_key,itm_associate2_dim_key,channel,hdr_tran_no
        order by trans_date
) L
where L.curr_customer_id not in 
     (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
     )""".format(*parameters)
    

    trans_df = t.read_data_from_snow_flake(sql_query)
    
    trans_df['brand_class'] = trans_df['brand_name'] + " " + trans_df['class_name']
    print('Transaction Table Customers: {:,}'.format(trans_df['curr_customer_id'].nunique()))
    return trans_df






def Daily_Channel_Based_Revenue(parameters, chain):
    parameters = parameters + [chain]

    sql_query = """
    Select date, channel, sum(trans_amount) revenue
    from
    (
    select distinct s.curr_cmd_id, s.hdr_business_date as date, s.channel, (s.itm_unit_net_purchase_amt*s.itm_qty) as trans_amount
        from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
        INNER JOIN nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
        where 
        (
        (s.hdr_business_date between '{0}' and '{1}')
        and (s.brand = '{2}')
        and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
        and (s.itm_item_status != 'V')
        and (s.curr_cmd_id != '0')
        )
    )X
    group by date, channel""".format(*parameters)
    
    df = t.read_data_from_snow_flake(sql_query)
    
    df.loc[:,'revenue'] = (df.loc[:,'revenue'].apply(pd.to_numeric))/1e6
    df.sort_values(by='date', inplace=True) 
    #df = df.reset_index(drop=True)
    df = pd.pivot_table(df, index=['date'], columns=['channel']).reset_index()
    if chain == 'NM':
        df.columns = ['date','catalog', 'online', 'store' ]
        df['total'] = (df[['catalog', 'online', 'store' ]].sum(axis=1))
    else:
        df.columns = ['date','online', 'store' ]
        df['total'] = (df[['online', 'store' ]].sum(axis=1))
          
    return df



def check_invidual_customer(cmd_id,from_date):
    # dept id 27 is 'Flash Sales Direct Purchase'
    query = """
    select distinct
    s.curr_cmd_id as customerid, s.hdr_business_date, s.order_id, l.location_name,
    s.pim_id, 
    s.itm_unit_net_purchase_amt, s.itm_qty, s.dsc_unit_discount_amt, s.itm_retail_type,
    s.itm_associate_dim_key,s.itm_associate2_dim_key,
    p.division_name, p.department_name, p.group_name, p.class_name,
    p.brand_name, p.gender, p.color_desc, p.size_desc
    from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
     inner join nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
     inner join nmedwprd_db.pdwdm.location_dim l on (s.location_dim_key = l.location_dim_key)
     inner join nmedwprd_db.pdwdm.product_dim p on (s.pim_id = p.pim_id)
     where (
            (s.curr_cmd_id = '{0}')
             and (s.itm_item_status != 'V')
             and (s.hdr_business_date > '{1}')
             )
      order by s.hdr_business_date asc""".format(cmd_id,from_date)
    
    df = t.read_data_from_snow_flake(query)
    return df

def cust_store_trips(from_date, to_date):
    # dept id 27 is 'Flash Sales Direct Purchase'
    query = """
    select cmd_id, count(distinct hdr_business_date) as number_of_visits
    from
        (select distinct
        s.curr_cmd_id as cmd_id, s.channel, l.location_name,  s.hdr_business_date 
        from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
        inner join nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
        inner join nmedwprd_db.pdwdm.location_dim l on (s.location_dim_key = l.location_dim_key)
             where ( 
                (s.hdr_business_date between to_date('{0}') and to_date('{1}' ))
                and (s.brand = 'NM')
                and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                and (s.itm_item_status != 'V')
                and (s.curr_cmd_id != '0')
                and (s.division_id != 27)
                and (s.curr_cmd_id not in
                    (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
                        ))
             )
             )
       where channel ='Store'
       group by cmd_id
     """.format(from_date,to_date)
    
    df = t.read_data_from_snow_flake(query)
    return df


def cust_channel_store_trips(from_date, to_date):
    # dept id 27 is 'Flash Sales Direct Purchase'
    query = """
    select cmd_id, channel, location_name as store_name,  count(distinct hdr_business_date) as number_of_visits
    from
        (select distinct
        s.curr_cmd_id as cmd_id, s.channel, l.location_name,  s.hdr_business_date 
        from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
        inner join nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
        inner join nmedwprd_db.pdwdm.location_dim l on (s.location_dim_key = l.location_dim_key)
             where ( 
                (s.hdr_business_date between to_date('{0}') and to_date('{1}' ))
                and (s.brand = 'NM')
                and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                and (s.itm_item_status != 'V')
                and (s.curr_cmd_id != '0')
                and (s.division_id != 27)
                and (s.curr_cmd_id not in
                    (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
                        ))
             )
             )
       group by cmd_id, channel, location_name
     """.format(from_date,to_date)
    
    df = t.read_data_from_snow_flake(query)
    return df

def cust_channel_division_trips(from_date, to_date):
    # dept id 27 is 'Flash Sales Direct Purchase'
    query = """
    select cmd_id, channel, division_id,  count(distinct hdr_business_date) as number_of_visits
    from
        (select distinct
        s.curr_cmd_id as cmd_id, s.channel, l.location_name, s.division_id, s.hdr_business_date 
        from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
        inner join nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
        inner join nmedwprd_db.pdwdm.location_dim l on (s.location_dim_key = l.location_dim_key)
             where ( 
                (s.hdr_business_date between to_date('{0}') and to_date('{1}' ))
                and (s.brand = 'NM')
                and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                and (s.itm_item_status != 'V')
                and (s.curr_cmd_id != '0')
                and (s.division_id != 27)
                and (s.curr_cmd_id not in
                    (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
                        ))
             )
             )
       group by cmd_id, channel, division_id
     """.format(from_date,to_date)
    
    df = t.read_data_from_snow_flake(query)
    return df

def Last_10_yrs_each_cust_daily_spent(parameters, New_Table_Flag = False):
    if New_Table_Flag == True:
        t.SF_read_sql( """
                    drop table if exists nmedwprd_db.mktsand.sk_last_10_yrs_each_cust_daily_total_spent;
                    create table nmedwprd_db.mktsand.sk_last_10_yrs_each_cust_daily_total_spent as
                    select 
                    curr_cmd_id as customerid, trans_date,  
                    sum(trans_amount) total_spent
                    from
                    (
                    select distinct s.curr_cmd_id, s.hdr_business_date as trans_date, s.hdr_tran_no, 
                    (s.itm_unit_net_purchase_amt*s.itm_qty) as trans_amount
                    from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
                    inner join nmedwprd_db.cmd.customer_t cet on (s.curr_cmd_id = cet.cmd_customer_id)
                    where ( 
                        (s.hdr_business_date between to_date('{0}') and to_date('{1}' ))
                        and (s.brand = 'NM')
                        and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                and (s.itm_item_status != 'V')
                and (s.curr_cmd_id != '0')
                and (s.division_id != 27)
                and (s.curr_cmd_id not in
                    (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
                        ))
                     )
                    )
                    group by 
                    curr_cmd_id, trans_date
                    having ((trans_date is not null) and (total_spent is not null))
                    order by curr_cmd_id, trans_date
                    """.format(*parameters))
        print("Table nmedwprd_db.mktsand.sk_last_10_yrs_each_cust_daily_total_spent is created!! \n")
    else: 
        print ("Taking data from past run table!\n")
        
    df = t.SF_read_sql('''select *  from nmedwprd_db.mktsand.sk_last_10_yrs_each_cust_daily_total_spent''')
    df['total_spent'] = df['total_spent'].astype('int64')
    df['customerid'] = df['customerid'].astype(str)
    df['trans_date'] =  pd.to_datetime(df['trans_date'])
    
    print("Number of records: {:,}".format(df.shape[0]))
    print('Transaction Table Customers: {:,}'.format(df['customerid'].nunique()))
    return df

def RFM(New_Table_Flag = False):
    
    if New_Table_Flag == True:
        t.SF_read_sql("""
            drop table if exists nmedwprd_db.mktsand.sk_RFM;
            create table nmedwprd_db.mktsand.sk_RFM as
            SELECT customerid, 
            COUNT(distinct date(trans_date)) - 1 as frequency,
            datediff('day', MIN(trans_date), MAX(trans_date)) as days_betn_first_shopd_and_last_shopd,
            AVG(total_spent) as avg_monetary_value,
            SUM(total_spent) as now_lifetime_spent,
            datediff('day', MIN(trans_date), CURRENT_DATE) as t_first_day_to_today,
            datediff('day', MAX(trans_date), CURRENT_DATE) as days_since_last_shopd,
            MIN(trans_date) first_shopd_day,
            MAX(trans_date) last_shopd_day
            FROM nmedwprd_db.mktsand.sk_last_10_yrs_each_cust_daily_total_spent
            GROUP BY customerid """)
        print("Table nmedwprd_db.mktsand.sk_RFM is created and taking data from new table!! \n")
    else: 
        print ("Taking data from past run table!\n")
        
    query = "select *  from nmedwprd_db.mktsand.sk_RFM"
    df = t.SF_read_sql(query)
    df = df.astype({'customerid':'str',
                  'frequency': 'float',
                  'days_betn_first_shopd_and_last_shopd': 'float',
                  't_first_day_to_today': 'float'
                  })

    df['first_shopd_day'] =  pd.to_datetime(df['first_shopd_day'])
    df['last_shopd_day'] =  pd.to_datetime(df['last_shopd_day'])
    print('Unique Customers: {:,}'.format(df['customerid'].nunique()))
    #to_date(localtimestamp())
    return df

def create_location_inter_shooping_day_features_table(parameters, New_Table_Flag =False):
    if New_Table_Flag == True:
        t.SF_read_sql("""
        drop table if exists nmedwprd_db.mktsand.sk_inter_shooping_day_features;
        create table nmedwprd_db.mktsand.sk_inter_shooping_day_features as
        select L.customerid, L.trans_date,  
        datediff('day', L.lagged_trans_date, L.trans_date) as inter_shopping_days,
        L.trip_number,
        case
            when location_name like '%Online%' then 'online'
            when location_name like '%Catalog%' then 'catalog'
            else location_name
        end as location_name,  
        L.spent_in_this_shopd_day,  
        L.last_shopd_day,
        (to_date(localtimestamp())-L.last_shopd_day) as days_since_last_shopd,
        L.first_shopd_day,  
        datediff('day', L.first_shopd_day, L.last_shopd_day) as days_betn_first_shopd_and_last_shopd 
        from
        (
        select customerid, trans_date, location_name,
        lag(trans_date) over    (partition by customerid order by trans_date) as lagged_trans_date,
        count(trans_date) over  (partition by customerid order by trans_date) as trip_number,
        min(trans_date) over  (partition by customerid) as first_shopd_day,
        max(trans_date) over  (partition by customerid) as last_shopd_day,
        spent_in_this_shopd_day
        from
            (
            select s.curr_cmd_id as customerid, s.hdr_business_date as trans_date, l.location_name,
            sum(s.itm_unit_net_purchase_amt*s.itm_qty) as spent_in_this_shopd_day
            from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
            inner join nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
            inner join nmedwprd_db.pdwdm.location_dim l on (s.location_dim_key = l.location_dim_key)
            where (
                (s.hdr_business_date between to_date('{0}') and to_date('{1}' ))
                and (s.brand = '{2}')
                and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                and (s.itm_item_status != 'V')
                and (s.curr_cmd_id != '0')
                and (s.division_id not in (27,77))
                )
         group by s.curr_cmd_id, s.hdr_business_date, l.location_name   
             )
         ) L
         where ((days_since_last_shopd < {3}) 
         and
         (L.customerid not in 
         (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
      ))
     )
     order by L.customerid, L.trans_date
     """.format(*parameters))
        print("Table nmedwprd_db.mktsand.sk_inter_shooping_day_features is created. \n")
    else: 
        print ("""Table nmedwprd_db.sk_inter_shooping_day_features may have old data. 
        Set New_Table_Flag to True for creating new table\n""")
    return 



def create_inter_shooping_day_features_table(parameters, New_Table_Flag =False):
    if New_Table_Flag == True:
        t.SF_read_sql("""
        drop table if exists nmedwprd_db.mktsand.sk_inter_shooping_day_features;
        create table nmedwprd_db.mktsand.sk_inter_shooping_day_features as
        select L.customerid, L.trans_date,  
        datediff('day', L.lagged_trans_date, L.trans_date) as inter_shopping_days,
        L.trip_number,
        L.spent_in_this_shopd_day,  
        L.last_shopd_day,
        (to_date(localtimestamp())- L.last_shopd_day) as days_since_last_shopd,
        L.first_shopd_day,  
        datediff('day', L.first_shopd_day, L.last_shopd_day) as days_betn_first_shopd_and_last_shopd 
        from
        (
        select customerid, trans_date,
        lag(trans_date) over    (partition by customerid order by trans_date) as lagged_trans_date,
        count(trans_date) over  (partition by customerid order by trans_date) as trip_number,
        min(trans_date) over  (partition by customerid) as first_shopd_day,
        max(trans_date) over  (partition by customerid) as last_shopd_day,
        spent_in_this_shopd_day
        from
            (
            select s.curr_cmd_id as customerid, s.hdr_business_date as trans_date,
            cast(sum(s.itm_unit_net_purchase_amt*s.itm_qty) as double) as spent_in_this_shopd_day
            from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
            inner join nmedwprd_db.cmd.customer_t cet ON (s.curr_cmd_id = cet.cmd_customer_id)
            where (
                (s.hdr_business_date between to_date('{0}') and to_date('{1}' ))
                and (s.brand = '{2}')
                and (s.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                and (s.itm_item_status != 'V')
                and (s.curr_cmd_id != '0')
                and (s.division_id not in (27,77))
                )
         group by s.curr_cmd_id, s.hdr_business_date  
             )
         ) L
         where ((days_since_last_shopd < {3}) 
         and
         (L.customerid not in 
         (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
      ))
     )
     order by L.customerid, L.trans_date
     """.format(*parameters))
        print("Table nmedwprd_db.mktsand.sk_inter_shooping_day_features is created. \n")
    else: 
        print ("""Table nmedwprd_db.sk_inter_shooping_day_features may have old data. 
        Set New_Table_Flag to True for creating new table\n""")
    return 


def New_RFM(New_Table_Flag = False):
    ## Need to improve for return...
    
    if New_Table_Flag == True:
        t.SF_read_sql("""
            drop table if exists nmedwprd_db.mktsand.sk_new_RFM;
            create table nmedwprd_db.mktsand.sk_new_RFM as
            select customerid,
            avg(spent_in_this_shopd_day) as avg_monetary_value,
            sum(spent_in_this_shopd_day) as now_lifetime_spent,
            count(distinct date(trans_date)) - 1 as frequency,
            datediff('day', min(trans_date), max(trans_date)) as days_betn_first_shopd_and_last_shopd, /* it is known lifetime */

            datediff('day', min(trans_date), CURRENT_DATE) as days_betn_first_shopd_and_today,
            datediff('day', max(trans_date), CURRENT_DATE) as days_since_last_shopd,
            min(trans_date) first_shopd_day,
            max(trans_date) last_shopd_day
            from nmedwprd_db.mktsand.sk_inter_shooping_day_features
            group by customerid """)
        print("Table nmedwprd_db.mktsand.sk_new_RFM is created and taking data from new table ... \n")
    else: 
        print ("Taking data from past run table...\n")
        
    query = "select *  from nmedwprd_db.mktsand.sk_new_RFM"
    df = t.SF_read_sql(query)
    df = df.astype({'customerid':'str',
                  'frequency': 'int',
                  'days_betn_first_shopd_and_last_shopd': 'float',
                  'days_betn_first_shopd_and_today': 'float',
                  'first_shopd_day' :'datetime64[D]',
                  'last_shopd_day' :'datetime64[D]'
                  })

    # df['first_shopd_day'] =  pd.to_datetime(df['first_shopd_day'])
    # df['last_shopd_day'] =  pd.to_datetime(df['last_shopd_day'])
    print('Unique Customers: {:,}'.format(df['customerid'].nunique()))
    #to_date(localtimestamp())
    return df


def all_recent_cust_daily_location_spent_product_info(New_Table_Flag = False): 
    # Fixed: Remove location_name (None table contains this attribite)
    
    if New_Table_Flag == True:
        t.SF_read_sql( """
        drop table if exists nmedwprd_db.mktsand.sk_all_recent_cust_daily_location_spent_product_info;
        create table nmedwprd_db.mktsand.sk_all_recent_cust_daily_location_spent_product_info as
        select distinct s.curr_cmd_id as customerid,  s.hdr_business_date as trans_date, v.inter_shopping_days, v.trip_number, 
        v.first_shopd_day, v.last_shopd_day, v.days_since_last_shopd, 
        v.days_betn_first_shopd_and_last_shopd,
        s.pim_id, s.itm_unit_net_purchase_amt, s.itm_qty, s.dsc_unit_discount_amt, s.itm_unit_retail_price,
        p.initial_cost, p.initial_retail, 
        case 
        when p.division_name like 'Wo%' then 'WA'
        when p.division_name like 'Co%' then 'CA'
        when p.division_name like 'Ch%' then 'Ch'
        when p.division_name like 'Gi%' then 'GH'
        when p.division_name like 'La%' then 'LS'
        when p.division_name like 'De%' then 'DH'
        when p.division_name like 'Fa%' then 'FA'
        when p.division_name like 'Me%' then 'Men'
        when p.division_name like 'Be%' then 'Be'
        when p.division_name like 'Je%' then 'Je'
        when p.division_name like 'Fi%' then 'FA'
        when p.division_name like 'No%' then 'NR'
        when p.division_name like 'In%' then 'IA'
        when p.division_name like 'NMG Mis%' then 'Mis'
        else p.division_name
        end as division_name, 
        p.department_name, p.group_name, p.class_name,
        case
        when p.brand_name like 'UNKNOWN%' then '?'
        else p.brand_name
        end brand_name,
        case
        when p.gender like 'W%' then 'W'
        when p.gender like 'U%' then 'U'
        when p.gender like 'M%' then 'M'
        when p.gender like 'G%' then 'G'
        when p.gender like 'B%' then 'B'
        else 'N'
        end as gender, 
        case
        when p.color_desc like 'NO COLO%' then '?'
        else p.color_desc
        end as color, 
        case
        when p.size_desc like 'NO SIZ%' then '?'
        else p.size_desc
        end as size
         from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
         inner join nmedwprd_db.pdwdm.product_dim p on (s.pim_id = p.pim_id)
         inner join nmedwprd_db.mktsand.sk_inter_shooping_day_features v 
         on ((s.curr_cmd_id = v.customerid) and (s.hdr_business_date = v.trans_date ))
          where (s.itm_item_status != 'V')""")  
        print("Created Table nmedwprd_db.mktsand.sk_all_recent_cust_daily_location_spent_product_info \n")
    else:
        print("Table nmedwprd_db.mktsand.sk_all_recent_cust_daily_location_spent_product_info may have old data.\n")
    return 

def pim_id_price_product_description(parameters):
    query = """
        select distinct 
        s.pim_id, s.itm_unit_net_purchase_amt, s.dsc_unit_discount_amt, s.itm_unit_retail_price,
        p.brand_name, p.class_name, p.initial_cost, p.initial_retail, p.tax_category_id, p.gender, p.color_desc, p.size_desc,
        p.class_name, p.department_name, p.division_name, p.group_name
                    from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
                    inner join nmedwprd_db.mktsand.sk_recent_cust_daily_location_pim_spent ps on (s.pim_id = ps.pim_id)
                    inner join nmedwprd_db.pdwdm.product_dim p on (s.pim_id = p.pim_id)
                    where (
                    (s.hdr_business_date between '{0}' and '{1}')
                    and (s.brand = '{2}')
                    )
         """.format(*parameters)
    df = t.SF_read_sql(query)
    print('Transaction Table  pim ids: {:,}'.format(df['pim_id'].nunique()))
    return df

def cohort_recent_cust_daily_location_spent_product_info():
    # dept id 27 is 'Flash Sales Direct Purchase'
    query = """
    select distinct 
    A.customerid, A.trans_date,
    A.location_name, A.delta_days, A.curr_freq, A.first_day, A.last_day, A.recency, A.t_alive,
    A.pim_id, 
    A.itm_unit_net_purchase_amt, A.itm_qty, A.dsc_unit_discount_amt, A.itm_unit_retail_price,
    A.initial_cost, A.initial_retail, A.division_name, 
    A.department_name, A.group_name, A.class_name,
    A.brand_name, A.gender, A.color, A.size
          from nmedwprd_db.mktsand.sk_all_recent_cust_daily_location_spent_product_info A
    inner join nmedwprd_db.mktsand.sk_cohort_customer_trans_date B 
    on ((A.customerid = B.customerid) and (A.trans_date = B.trans_date ))
    order by customerid, trans_date
    """
    df = t.SF_read_sql(query)
    df['trans_date'] = pd.to_datetime(df['trans_date'])
    print('Transaction Table Customers: {:,}'.format(df['customerid'].nunique()))
    
    return df


def Resa_Sale_daily_revenue_customer(parameters):
    sql_query = """
    select hdr_business_date date, sum(trans_amount)/1e6 as revenue_resa
    from 
    (
    select distinct s.hdr_business_date, c.curr_customer_id, s.itm_unit_net_purchase_amt*s.itm_qty as trans_amount                
            from nmedwprd_db.pdwdm.resa_sales_fact s
            INNER JOIN nmedwprd_db.pdwdm.cmd_customer_dim c ON (s.cmd_customer_dim_key = c.cmd_customer_dim_key)
            INNER JOIN nmedwprd_db.pdwdm.location_dim l ON (s.Location_dim_key = l.Location_dim_key)
            INNER JOIN nmedwprd_db.cmd.customer_t cet ON (c.curr_customer_id = cet.cmd_customer_id)
            where 
            (
            (s.hdr_business_date between '{0}' and '{1}')
            and ((l.area_id = 14) or (l.region_id=201) or (l.district_id in (13011, 13021, 13014)))
            and(s.itm_dept_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
            and(s.itm_item_status != 'V')
            and (c.curr_customer_id !='0')
            )
    ) X
    group by hdr_business_date""".format(*parameters)
    
    #resa_df=pd.DataFrame(scur.fetchall(), columns=['date','revenue_resa'])
    resa_df = t.read_data_from_snow_flake(sql_query)
    resa_df = resa_df.sort_values(by='date').reset_index(drop=True)
    return resa_df

def create_rolling_12mo_sales_table(parameters):
    
    this_year_month = pd.read_sql("""
    select max(fiscal_month_end) latest_year_month
    from nmedwprd_db.pdwdm.sas_sales_cmd_rolling_summary_v 
    where brand = '{0}' and fiscal_month_end > '{1}'""".format(*parameters), sf_cnx).iloc[0,0]
    last_year_month = this_year_month - 100
    print('This year month: ', this_year_month)
    print('Last year month: ', last_year_month)
    parameters = parameters + [this_year_month, last_year_month]
    
    rolling_TY_LY = pd.read_sql( """
    select TY.curr_customer_id, TY.TY_rolling_amt, TY.TY_cgroup, LY.LY_rolling_amt, LY.LY_cgroup
        from
        (
        select distinct curr_customer_id, rolling_12mo_sales_amt TY_rolling_amt, customer_segment TY_cgroup
        from nmedwprd_db.pdwdm.sas_sales_cmd_rolling_summary_v 
        where brand = '{0}' and fiscal_month_end = '{2}'
        ) TY
        left join
        (
        select distinct curr_customer_id, rolling_12mo_sales_amt LY_rolling_amt, customer_segment LY_cgroup
        from nmedwprd_db.pdwdm.sas_sales_cmd_rolling_summary_v 
        where brand = '{0}' and fiscal_month_end = '{3}'
        ) LY
        on TY.curr_customer_id = LY.curr_customer_id
        """.format(*parameters),sf_cnx)

    rolling_TY_LY.columns = rolling_TY_LY.columns.str.lower()
    rolling_TY_LY = rolling_TY_LY.drop_duplicates(subset='curr_customer_id', keep="first")
    rolling_TY_LY = rolling_TY_LY.fillna(0)
    print('Rolling Table customers: {:,}'.format(rolling_TY_LY['curr_customer_id'].nunique()))

    return rolling_TY_LY

def take_transaction_with_brand_class(parameters):

    ## note this preclude how many same item is purchased per day
    t.SF_read_sql("""
 select X.* from
(select distinct s.curr_cmd_id curr_customer_id, s.pim_id, s.hdr_business_date event_date, p.brand_name brand, p.class_name class
                    from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
                    inner join nmedwprd_db.cmd.customer_t cet on  (s.curr_cmd_id = cet.cmd_customer_id)
                    inner join pdwdm.product_dim p on (s.pim_id = p.pim_id)
                    where (
                    (s.hdr_business_date between '{1}' and '{2}')
                    and (s.brand = '{3}')
                    and (p.department_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
                    and (s.itm_item_status != 'V')
                    and (s.curr_cmd_id != '0')
                    )
                    
      ) X
where X.curr_customer_id not in
    (select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
    )""".format(*parameters))
    
    df=pd.DataFrame(scur.fetchall())
    df.columns=['curr_customer_id', 'pim_id', 'event_date',  'brand', 'class']
    print('Trans  customers: {:,}'.format(df['curr_customer_id'].nunique()))

    return df



def take_web_browse_with_brand_class(parameters):

    ## note this preclude how many same item is browsed per day
    sql_query = """
    select X.* from
    (select distinct L.curr_customer_id, R.cmos_item, R.event_date, R.brand_name, R.class
    from 
        (select curr_customer_id, email from nmedwprd_db.pdwdm.cmd_email_to_customer_t) L
        inner join
        (select distinct A.email, A.cmos_item cmos_item, A.event_date, B.brand_name, B.merchant_class class
         from (select email,cmos_item, event_date
                 from (select post_evar49 as email, substr(split(product_list, ';')[1],-5) as cmos_item, event_date
                      from 
                          (select post_evar49, product_list, event_date
                            from nmedwprd_db.pdwdm.om_consolidated_subset_v
                            where 
                            (
                            (post_evar49 is not null) and (event_date > '{0}')
                            and (brand = '{2}') and (exclude_hit = 0) 
                            and (product_list is not NULL) and (duplicate_purchase = 0)
                            and (purchaseid is null)and (lower(pagename) like '%detail%')
                            )
                         ) tst, LATERAL FLATTEN (INPUT => SPLIT(tst.product_list,',')) s
                    ) group by email, cmos_item, event_date
            ) A 
            inner join
            (
            select distinct cmos_item_code,brand_name, merchant_class 
            from nmedwprd_db.pdwdm.catalog_item_dim
            ) B on A.cmos_item = B.cmos_item_code
        ) R on L.email = R.email
) X
where X.curr_customer_id not in
(select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                        where substr(SUPPRESSION_STRING,42,1) 
                           or substr(SUPPRESSION_STRING,1,1)
                           or substr(SUPPRESSION_STRING,3,1)
                           or substr(SUPPRESSION_STRING,5,1)
                           or substr(SUPPRESSION_STRING,6,1)
                           or substr(SUPPRESSION_STRING,7,1)
)""".format(*parameters)
    
    df=t.read_data_from_snow_flake(sql_query)
    df.columns = ['curr_customer_id', 'cmos_item','event_date','brand','class']
    print('Browse customers: {:,}'.format(df['curr_customer_id'].nunique()))
    return df

def browse_customer_brand_class(param):
    df = take_web_browse_with_brand_class(param)
    df['brand_class'] = df['brand'] + "_" + df['class']
    df = df.groupby(['curr_customer_id','brand_class']).size().reset_index(name="times_browse")
    return df

def trans_customer_brand_class(param):
    df = take_transaction_with_brand_class(param)
    df['brand_class'] = df['brand'] + "_" + df['class']
    df = df.groupby(['curr_customer_id','brand_class']).size().reset_index(name="total_transactions")
    return df


def all_SA_and_location(chain):
    # All SA DF
    SA_df = SA_profile()
    print('Total SAs from pdwdm.associate_dim table: {:,} '.format(SA_df['sa_pin'].nunique()))
    if chain == 'NM':
        location_df = select_NM_locations()
    elif chain == 'BG':
        location_df = select_BG_locations()
    else:
        print('Enter either NM or BG as chain')
        
    SA_Location_df = pd.merge(SA_df, location_df,how='inner',on=['SA_location_id'])
    print('For {} chain .. SAs: {:,} ... locations: {} '.format(chain, SA_Location_df['sa_pin'].nunique(), SA_Location_df['SA_location_id'].nunique()))
   
    return SA_Location_df

def SA_profile():
    
    sql_query = """select distinct associate_dim_key,associate_pin,first_name,last_name,hr_department_desc,
                            hr_department_id, work_location_id 
                            from pdwdm.associate_dim"""
    SA_df=t.read_data_from_snow_flake(sql_query)
    SA_df.columns = ['itm_associate_dim_key','sa_pin','SA_first_name','SA_last_name','SA_department', 'SA_department_id', 'SA_location_id']
    SA_df = SA_df.dropna(subset=['SA_location_id'])
    SA_df['SA_location_id'] = SA_df['SA_location_id'].astype('int64')
    return SA_df

def select_BG_locations():
       
    sql_query = """
    select distinct hdr_store_id as sa_location_id
    from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v 
    where brand = 'BG'"""
    
    df=t.read_data_from_snow_flake(sql_query).dropna()
    df['SA_location_id'] = df['SA_location_id'].astype('int64')
    return df

def select_NM_locations():

    sql_query = """
                    select distinct LD.location_id as SA_location_id
                    from mktsand.location_channel_v LC
                    inner join pdwdm.location_dim LD 
                    on LC.location_dim_key = LD.location_dim_key
                    where channel in ('NM_store','NM_online','NM_catalog')"""
    
    df = t.read_data_from_snow_flake(sql_query).dropna()
    df['SA_location_id'] = df['SA_location_id'].astype('int64')
    return df


def lifecycle_data():
    df = t.read_a_file_from_s3('{0}_cust_lifecycle.csv'.format(h.wbrand),h.read_dir_nameX, h.bucket_nameX)
    df = df[['curr_customer_id','total_spent','level']]
    df = df.drop_duplicates(['curr_customer_id'])
    print('LifeCycle customers: {:,} '.format(df['curr_customer_id'].nunique()))
    return df

def Exclude_VIP_Digital_InactiveSA_Exclusions(trans_df1):
        
    print('---------------------------------------------------------')
    print('Excluding VIP DLE customers ...')
    trans_df2 = exclude_NM_DLE_customers(trans_df1)
    #print('After DLE exclusion -- Customers: {:,}'.format(trans_df2['curr_customer_id'].nunique()))
    
    
    print('----------------------------------------------------------')
    # Digital/DCA customers
    print('Excluding and saving digital customers')
    digital_customers = collect_digital_customers(h.parameters_digital)
    digital_N = digital_customers['curr_customer_id'].nunique()
    print("Digital customers {:,}".format(digital_N))
    t.save_df_to_s3(digital_customers, '{}_digital_client_list.csv'.format(h.wbrand), h.save_dir_nameX, h.bucket_nameX)
    # remove digital/DCA customer ...
    trans_df3 = t.remove_df_based_on_key(trans_df2, digital_customers, ['curr_customer_id'])
    non_digital = trans_df3['curr_customer_id'].nunique()
    print('After digital exclusion-- customers: {:,} cust reduction {}%'.format(non_digital,math.ceil(digital_N/(non_digital+ digital_N)*100 )))
    
    
    print ('--------------------------------------------------------------------')
    print('Keeping active associates transactions')
    # Active SA DF
    ActiveSA_list = t.read_a_file_from_s3('Active_SAs.csv', 'ds/prod/NewRTC/CMD/data/nm/', 'nmg-analytics-ds-prod')
    print('Active SAs: {:,} '.format(ActiveSA_list['sa_pin'].nunique()))
    # Keeping only transaction records from active SAs
    trans_df4 = keep_cust_transaction_with_active_SA(trans_df3, ActiveSA_list)
    
    cust3 = trans_df3['curr_customer_id'].nunique()
    cust4 = trans_df4['curr_customer_id'].nunique()
    SA1 = trans_df4['itm_associate_dim_key'].nunique()
    SA2 = trans_df4['itm_associate2_dim_key'].nunique()
    print('After Inactive SA exclusion -- customers {:,} -- Cust loss {}% -- SA1 {}  -- SA2 {}'.format(cust4, math.ceil(100*(cust3-cust4)/cust3), SA1,SA2))
    print('!!!! Looks like it removes inactives from the SA1 customers but does not remove all inactive SAs some SA2???')
    return trans_df4

def transactions_after_VIP_Digital_InactiveSA_Exclusions():
    # Transaction DF
    print('---------------------------------------------------------')
    print('Loading data, please wait ...')
    trans_df1 = t.read_a_file_from_s3('{}_trans_df.csv'.format(h.wbrand), h.read_dir_nameX, h.bucket_nameX)
    print('Trans DF -- Customers: {:,}'.format(trans_df1['curr_customer_id'].nunique()))
    
    
    print('---------------------------------------------------------')
    print('Excluding VIP DLE customers ...')
    trans_df2 = exclude_NM_DLE_customers(trans_df1)
    #print('After DLE exclusion -- Customers: {:,}'.format(trans_df2['curr_customer_id'].nunique()))
    
    
    print('----------------------------------------------------------')
    # Digital/DCA customers
    print('Excluding and saving digital customers')
    digital_customers = collect_digital_customers(h.parameters_digital)
    digital_N = digital_customers['curr_customer_id'].nunique()
    print("Digital customers {:,}".format(digital_N))
    t.save_df_to_s3(digital_customers, '{}_digital_client_list.csv'.format(h.wbrand), h.save_dir_nameX, h.bucket_nameX)
    # remove digital/DCA customer ...
    trans_df3 = t.remove_df_based_on_key(trans_df2, digital_customers, ['curr_customer_id'])
    non_digital = trans_df3['curr_customer_id'].nunique()
    print('After digital exclusion-- customers: {:,} cust reduction {}%'.format(non_digital,math.ceil(digital_N/(non_digital+ digital_N)*100 )))
    
    
    print ('--------------------------------------------------------------------')
    print('Keeping active associates transactions')
    # Active SA DF
    ActiveSA_list = t.read_a_file_from_s3('Active_SAs.csv', 'ds/prod/NewRTC/CMD/data/nm/', 'nmg-analytics-ds-prod')
    print('Active SAs: {:,} '.format(ActiveSA_list['sa_pin'].nunique()))
    # Keeping only transaction records from active SAs
    trans_df4 = keep_cust_transaction_with_active_SA(trans_df3, ActiveSA_list)
    
    cust3 = trans_df3['curr_customer_id'].nunique()
    cust4 = trans_df4['curr_customer_id'].nunique()
    SA1 = trans_df4['itm_associate_dim_key'].nunique()
    SA2 = trans_df4['itm_associate2_dim_key'].nunique()
    print('After Inactive SA exclusion -- customers {:,} -- Cust loss {}% -- SA1 {}  -- SA2 {}'.format(cust4, math.ceil(100*(cust3-cust4)/cust3), SA1,SA2))
    print('!!!! Looks like it removes inactives from the SA1 customers but does not remove all inactive SAs some SA2???')
    return trans_df4

def exclude_NM_DLE_customers(df_before):
    # Grabbing DLE/ NM VIP customers to exclude them 
    sql_query = """select cmd_customer_id
                    from nmedwprd_db.cmd.customer_source_t cst
                    join nmedwprd_db.cmd.customer_t ct ON (ct.customer_key = cst.customer_key)
                    where customer_source = 'DLE' """
    exclusion = t.read_data_from_snow_flake(sql_query)
    keys = ['curr_customer_id']
    exclusion.columns = keys
    print('VIP customers: {:,} '.format(exclusion['curr_customer_id'].nunique()))
    
    df_after = t.remove_df_based_on_key(df_before, exclusion, keys)
    return df_after


def collect_digital_customers(parameters):
    # Grabing customers with itm_associate_dim_key = -1
    

    sql_query = """
select L.curr_customer_id from
    (
    select distinct curr_cmd_id as curr_customer_id
     from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
     inner join NMEDWPRD_DB.cmd.customer_t cet on (s.curr_cmd_id = cet.cmd_customer_id)
     where (
        (s.hdr_business_date between '{0}' and '{1}')
        and (s.brand = '{2}') 
        and (s.itm_item_status != 'V')
        and (s.itm_associate_dim_key = '-1')
        and (s.curr_cmd_id != '0')
        )
        ) L
 where L.curr_customer_id not in
        (
            select distinct curr_cmd_id as curr_customer_id
     from nmedwprd_db.pdwdm.all_sas_sales_and_open_orders_v s
     inner join NMEDWPRD_DB.cmd.customer_t cet on (s.curr_cmd_id = cet.cmd_customer_id)
     where (
        (s.hdr_business_date between '{0}' and '{1}')
        and (s.brand = '{2}') 
        and (s.itm_item_status != 'V')
        and (s.itm_associate_dim_key != '-1')
        and (s.curr_cmd_id != '0')
        )
        ) """.format(*parameters)
    
    cust_NoSA=t.read_data_from_snow_flake(sql_query)
    
    return cust_NoSA

def keep_cust_transaction_with_active_SA(trans_df, ActiveSA_list ):
    
    trans_df_1 = pd.merge(trans_df, ActiveSA_list[['itm_associate_dim_key']],  how='inner', on='itm_associate_dim_key')
    trans_df_2 = pd.merge(trans_df, ActiveSA_list[['itm_associate_dim_key']],  how='inner', left_on='itm_associate2_dim_key', right_on='itm_associate_dim_key')
    ### !!!! may need to do join as opposed to concat
    trans_df = pd.concat([trans_df_1, trans_df_2], axis=0, sort=True)
    # Deleting uneeded columns to make df cleaner
    del trans_df['itm_associate_dim_key_x']
    del trans_df['itm_associate_dim_key_y']
    
    trans_df["itm_associate_dim_key"].fillna(-1, inplace = True)
    trans_df["itm_associate2_dim_key"].fillna(-1, inplace = True)
    trans_df['itm_associate_dim_key'] = trans_df['itm_associate_dim_key'].astype('int64')
    trans_df['itm_associate2_dim_key'] = trans_df['itm_associate2_dim_key'].astype('int64')
    
    trans_df = trans_df.drop_duplicates(['curr_customer_id', 'trans_date', 'brand_name', 'class_name', 'pim_id',
       'location_dim_key', 'department_id', 'department_name', 'division_id',
       'itm_associate_dim_key', 'itm_associate2_dim_key', 'channel', 'hdr_tran_no',
        'total_spent'])
    
    return trans_df




def customer_profile():

    sql_query = """select distinct curr_customer_id, prefix_name custPrefix, 
    first_name custFirstName, middle_initial custMiddleINIT, last_name custLastName
                            from nmedwprd_db.pdwdm.cmd_customer_dim"""
    df = t.read_data_from_snow_flake(sql_query)
    df = df.dropna(subset=['curr_customer_id'])
    return df



def find_store_location_name():
   
    df = t.read_data_from_snow_flake("""select location_id, location_name from pdwdm.location_dim""")
    return df


def store_inventory(parameters):
    
    sf_cnx = sf_connection()
    scur= sf_cnx.cursor()

    scur.execute("""DROP TABLE IF EXISTS MKTSAND.{0}_temp_inventory""".format(*parameters))
    scur.execute( """
        create table MKTSAND.{0}_temp_inventory as
        SELECT distinct Location_dim_key,brand_class, sum(onhand_minus_suppression_qty) as brand_class_qty
                from 
                (SELECT distinct
                I.onhand_minus_suppression_qty,
                I.Location_dim_key,
                concat(p.brand_name, '_' ,p.class_name)  as brand_class             
                FROM pdwdm.inv_hub_store_inventory_v I, pdwdm.product_dim p
                where (
                    (I.pim_id = p.pim_id) and (IN_SELLING_GROUP = '{0}STORES')
                    and (BLOCK_STORE_FLAG = 'FALSE')
                   )
                ) X
                group by Location_dim_key,brand_class""".format(*parameters))
    Inv = pd.read_sql('''select *  from MKTSAND.{0}_temp_inventory'''.format(*parameters), sf_cnx)
    Inv.columns = Inv.columns.str.lower()
    
    scur.close()
    sf_cnx.close()
    
    return Inv

def online_inventory(wbrand):

    t.SF_read_sql(""" SELECT distinct brand_class, sum(WEB_QTY_IN_STOCK) as brand_class_qty
                from 
                (SELECT distinct
                I.WEB_QTY_IN_STOCK,
                concat(p.brand_name, ' ' ,p.class_name)  as brand_class             
                FROM NMEDWPRD_DB.PDWDM.WEB_SKU_INVENTORY_FACT I, pdwdm.product_dim p
                where (
                    (I.CUSTOM_NMG_PIM_ID = p.pim_id) and (retailer_code = '{0}')
                   )
                ) X
                group by brand_class """.format(wbrand))
    OInventory = pd.DataFrame(scur.fetchall())
    OInventory.columns=['brand_class','online_brand_class_qty']
    return OInventory

def Merge_Store_and_Online_Inventory(InvS, InvO):
    Inventory = pd.merge(InvS, InvO,  how='outer', on=['brand_class'])
    Inventory[['brand_class_qty', 'online_brand_class_qty']] = Inventory[['brand_class_qty', 'online_brand_class_qty']].fillna(value=0)
    Inventory = Inventory.drop_duplicates(['Location_dim_key','brand_class'])
    Inventory['brand_class_qty'] = Inventory['brand_class_qty'] + Inventory['online_brand_class_qty']
    del Inventory['online_brand_class_qty']
    Inventory['Location_dim_key'].fillna(124, inplace = True) 
    Inventory['Location_dim_key'] = Inventory['Location_dim_key'].astype('int64')
    return Inventory

def Inventory_to_remove(Inventory, N=10):
    sf_cnx = sf_connection()
  
    Inventory2remove = Inventory[Inventory['brand_class_qty'] < N]
    Inventory2remove = Inventory2remove[['location_dim_key','brand_class']].copy()

    store_df = pd.read_sql("""select location_dim_key,location_id,location_name from pdwdm.location_dim """,sf_cnx)
    store_df = store_df.str.lower()

    Inventory2remove.columns = ['location_dim_key','brand_class']
    Inventory2remove = pd.merge(Inventory2remove, store_df ,how='inner',on='location_dim_key')
    Inventory2remove = Inventory2remove.dropna(subset=['location_id'])
    Inventory2remove['location_id'] = Inventory2remove['location_id'].astype('int64')
    del Inventory2remove['location_dim_key']
    del Inventory2remove['location_name']
    Inventory2remove = Inventory2remove.drop_duplicates()
    Inventory2remove = Inventory_to_remove
    
    sf_cnx.close()
    return Inventory2remove

def remove_inventory(df, Inventory2remove):
    df = t.remove_df_based_on_key(df, Inventory2remove)
    df = df[['curr_customer_id','brand_class']]
    df = df.drop_duplicates(['curr_customer_id','brand_class'])
    return df

def brands_class_exclusion(recs_df):
    remove_list = ["UNKNOWN BRAND","Trunk Show","Swimsuit","Swim","Unclassified","GWP","Bow Ties","Napkins","Travel","Ingestibles", "Socks", "Desserts", "Gentlemen?s", "Hardware", "Louis Vuitton","Christmas"]
    for e in remove_list:
        df_2_remove = recs_df[(recs_df.brand_class.str.contains(e))]
        recs_df = recs_df.drop(df_2_remove.index)
    return rec_df

def take_from_production_CFA_table():

    sql_query = """select cmd_id, segment, binning_ca_net_spend_12m 
    from NMEDWPRD_DB.CDP.cfa_segments_t 
    where 
    ((extract_date = '{0}') and (segment is not null) and (cmd_id is not null) and (binning_ca_net_spend_12m is not null))""".format(extract_date)
    df = t.read_data_from_snow_flake(sql_query)
  
    return df

def take_from_temp_CFA_table():
  
    sql_query = "select distinct file_date  from  NMEDWDEV_DB.CDP.CFA_SEGMENTS_T_TEMP2 where ((cmd_id is not null) and (curr_cmd_id is not null) and (binning_ca_net_spend_12m is not null))"
    df = t.read_data_from_snow_flake(sql_query)

    return df

def find_incircle_cmd_id_and_tender_id():
    query = """
    select distinct curr_customer_id as cmd_id, tender_id, tender_dim_key 
    from nmedwprd_db.pdwdm.cmd_customer_account_t scc
    inner join nmedwprd_db.cmd.customer_t cet on (scc.curr_customer_id = cet.cmd_customer_id)
    where scc.tender_type_id in ('BG','CHG')"""
    
    df = t.read_data_from_snow_flake(query)

    return df

def find_incircle_email_and_tender_id():
    query = """
    select email_address as email, mtf_ki as tender_id, max(mtf_rdate) as mtf_rdate 
    from crm.incircle_mtf_dim 
    where (
    ((email_address is not NULL) and  (email_address != 'None') and  (email_address != 'none@capitalone-no-email.com')) 
      and 
    (mtf_owner != 'AM')
    ) 
    group by email_address, mtf_ki"""
    
    df = t.read_data_from_snow_flake(query)
    return df

def find_incircle_cmd_id_and_email():
    df_cmd_id = find_incircle_cmd_id_and_tender_id()

    df_email  = find_incircle_email_and_tender_id()
   
    df_incircle = pd.merge(df_cmd_id, df_email[['tender_id','email']],  how='inner', on=['tender_id'])
    df_incircle = df_incircle.drop_duplicates(['cmd_id'])
    return df_incircle



def read_client_connect_ds_send_data(chain, begin_date):
    
    sql_query = """select cmd_id, sapinno as sa_pin, evtstartdte, evtenddte
    from NMEDWPRD_DB.mktsand.{0}_RTC_CDM_Table
    where evtstartdte >='{1}'  
    """.format(chain,begin_date)
    df = t.read_data_from_snow_flake(sql_query)
    
    df['date'] = h.pd.to_datetime(df['evtstartdte'], errors='coerce')
    df['week'] = df['date'].apply(lambda x:x.isocalendar()[1])
    df['week']=df['week'].apply(int)
    
    #convert column types to string
    df['evtstartdte'] = df['evtstartdte'].astype(str)
    df['evtenddte']   = df['evtenddte'].astype(str)
    return df

def add_omni_status(df):
    
    df_X = df[['cmd_id','channel']].drop_duplicates().sort_values(by='cmd_id')
    count_data = df_X['cmd_id'].value_counts()
    
    df.set_index('cmd_id', inplace=True)
    df['channel_count'] = count_data
    df.reset_index(inplace=True)
    
    df.loc[(df.channel_count > 1),'omni_status']='Omni'
    df.loc[((df.channel_count ==1) & (df.channel=='Store')),'omni_status'] = 'Store_Only'
    df.loc[((df.channel_count ==1) & (df.channel=='Online')),'omni_status'] ='Online_Only'
    df.loc[((df.channel_count ==1) & (df.channel=='Catalog')),'omni_status'] ='Catalog_Only'

    return df

def customer_favorite_store_basic_method(df):
    df1 = (df[df.channel=='Store']
           .sort_values(['cmd_id', 'location_name','number_of_visits'], ascending=False)
           .groupby('cmd_id')
           .head(1)[['cmd_id','location_name']]
           .drop_duplicates())
    
    df1.columns = ['cmd_id', 'favorite_store']
    return df1

def select_cust_cfa_cohorts():
    
    
    sql_query = """select curr_customer_id as cmd_id, cf_segment, cf_segment_0314, cf_rung_ladder, cf_customer_status,
    favorite_location_1, favorite_location_2
    from NMEDWPRD_DB.mktsand.NM_Customer_Metrics"""
    df= t.read_data_from_snow_flake(sql_query)
    
    
    # df.loc[(df.cf_segment==0)|(df.cf_rung_ladder=='0'),'lifecycle']='Undefined'    
    # df.loc[(df.cf_segment==1),'lifecycle']='Loyal'
    # df.loc[((df.cf_segment==2)|(df.cf_segment==3)|(df.cf_segment==4)|(df.cf_segment==5))&((df.cf_rung_ladder=='L1')|(df.cf_rung_ladder=='L2')|(df.cf_rung_ladder=='L3')),'lifecycle']='Transient'
    # df.loc[((df.cf_segment==2)| (df.cf_segment==3)|(df.cf_segment==4)|(df.cf_segment==5))&((df.cf_rung_ladder=='L4')|(df.cf_rung_ladder=='L5')|(df.cf_rung_ladder=='L6')),'lifecycle']='Loyal'
    # df.loc[(df.cf_segment==6),'lifecycle']='Transient'
    # df.loc[(df.cf_segment==7),'lifecycle']='Transient'
    # df.loc[(df.cf_segment==8),'lifecycle']='New'
    # df.loc[(df.cf_segment==9),'lifecycle']='New'
    # df.loc[(df.cf_segment==10),'lifecycle']='Transient'
    
    return df



def data_create_rolling_12mo_sales_table(parameters):
    
    this_year_month = pd.read_sql("""select max(fiscal_month_end) latest_year_month
                            from nmedwprd_db.pdwdm.sas_sales_cmd_rolling_summary_v 
                            where brand = '{0}' and fiscal_month_end > '{1}'""".format(*parameters), sf_cnx).iloc[0,0]
    last_year_month = this_year_month - 100
    print(this_year_month)
    print(last_year_month)
    parameters = parameters + [this_year_month, last_year_month]

    t.SF_read_sql( """
        drop table if exists MKTSAND.{0}_temp_rollingtable;
        create table MKTSAND.{0}_temp_rollingtable as
        select TY.curr_customer_id, TY.TY_rolling_12mo_sales_amt, TY.TY_cgroup, LY.LY_rolling_12mo_sales_amt, LY.LY_cgroup
        from
        (
        select distinct curr_customer_id, rolling_12mo_sales_amt TY_rolling_12mo_sales_amt, customer_segment TY_cgroup
        from nmedwprd_db.pdwdm.sas_sales_cmd_rolling_summary_v 
        where brand = '{0}' and fiscal_month_end = '{2}'
        ) TY
        left join
        (
        select distinct curr_customer_id, rolling_12mo_sales_amt LY_rolling_12mo_sales_amt, customer_segment LY_cgroup
        from nmedwprd_db.pdwdm.sas_sales_cmd_rolling_summary_v 
        where brand = '{0}' and fiscal_month_end = '{3}'
        ) LY
        on TY.curr_customer_id = LY.curr_customer_id
        """.format(*parameters))
    rolling_TY_LY = pd.read_sql('''select *  from MKTSAND.{0}_temp_rollingtable'''.format(*parameters), sf_cnx)
    rolling_TY_LY.columns = rolling_TY_LY.columns.str.lower()

    return rolling_TY_LY




def data_create_inventory_table(parameters):
    t.SF_read_sql("""
       DROP TABLE IF EXISTS MKTSAND.{0}_temp_inventory;
        create table MKTSAND.{0}_temp_inventory as
        SELECT distinct Location_dim_key,brand_class, sum(onhand_minus_suppression_qty) as brand_class_qty
                from 
                (SELECT distinct
                I.onhand_minus_suppression_qty,
                I.Location_dim_key,
                concat(p.brand_name, '_' ,p.class_name)  as brand_class             
                FROM pdwdm.inv_hub_store_inventory_v I, pdwdm.product_dim p
                where (
                    (I.pim_id = p.pim_id) and (IN_SELLING_GROUP = '{0}STORES')
                    and (BLOCK_STORE_FLAG = 'FALSE')
                   )
                ) X
                group by Location_dim_key,brand_class""".format(*parameters))
    Inv = pd.read_sql('''select *  from MKTSAND.{0}_temp_inventory'''.format(*parameters), sf_cnx)
    Inv.columns = Inv.columns.str.lower()
    return Inv


def select_500K_plus_cust(wbrand):
    sql_query = """ select distinct curr_customer_id,custPrefix,custFirstName,custMiddleINIT,custLastName,
        level,alive_probability,predicted_purchases,clv_frequency,clv_recency,clv_monetary_value,CF_segment,cf_customer_status from 
        nmedwprd_db.mktsand.{0}_Customer_Metrics
        where (DLE_flag = 0)
        and (associates_L24M >= 1)
        and (L12_FISCAL_MONTH_SPENT >= 500000)
        and (cust_loc_status != 'International')""".format(wbrand)
    
    df = t.read_data_from_snow_flake(sql_query)
    
    return df

def select_Client_list_from_Customer_Metrics(wbrand):
    sql_query = """ select distinct curr_customer_id,custPrefix,custFirstName,custMiddleINIT,custLastName,
        level,alive_probability,predicted_purchases,clv_frequency,clv_recency,clv_monetary_value,CF_segment,CF_customer_status from 
        nmedwprd_db.mktsand.{0}_Customer_Metrics
        where (DLE_flag = 0)
        and (associates_L24M >= 1)
        and (cust_loc_status != 'International')""".format(wbrand)
    df = t.read_data_from_snow_flake(sql_query)
    return df

def select_customers_1yr_postive_from_SA_Relationship_Metrics(wbrand):
    sql_query = """ select distinct curr_customer_id,itm_associate_dim_key,L24M_Trips,Relationship_L24M,SA_Net_Spent_L24M from 
        nmedwprd_db.mktsand.{0}_SA_Relationship_Metrics
        where SA_Net_Spent_L12M > 0 """.format(wbrand)
    df = t.read_data_from_snow_flake(sql_query)
    return df

def select_customers_1yr_negative_2yr_positive_from_SA_Relationship_Metrics(wbrand):
    sql_query =""" select distinct curr_customer_id,itm_associate_dim_key,L24M_Trips,Relationship_L24M,SA_Net_Spent_L24M from 
        nmedwprd_db.mktsand.{0}_SA_Relationship_Metrics
        where (SA_Net_Spent_L24M > 0) 
        and (SA_Net_Spent_L12M < 0)""".format(wbrand)
    df = t.read_data_from_snow_flake(sql_query)
    return df

def select_Active_SAs_with_L24M_customer_interaction(wbrand):
    sql_query =""" select distinct associate_pin,itm_associate_dim_key,SA_first_name,SA_last_name,SA_location_id,
                   SA_Age_Days, SA_revenue_L12M, Total_Transactions_L12M, customers_L12M
                   from 
                   nmedwprd_db.mktsand.{0}_SA_Metrics
                   where (status = 'Active')
                   and (customers_L24M >= 1) """.format(wbrand)
    df = t.read_data_from_snow_flake(sql_query)
    return df


def select_SA_pin_from_a_store(wbrand,store_id):
    sql_query = """ select distinct associate_pin from 
        nmedwprd_db.mktsand.{0}_SA_Metrics
        where (status = 'Active')
        and (customers_L24M >= 1)
        and (SA_location_id = {1})""".format(wbrand,store_id)
    df = t.read_data_from_snow_flake(sql_query)
    return df



    
def NM_daily_revenue_customer_from_resa_sales(parameters):
    sql_query = """
    select hdr_business_date date, sum(trans_amount)/1e6 as revenue_resa
    from 
    (
    select distinct s.hdr_business_date, c.curr_customer_id, s.itm_unit_net_purchase_amt*s.itm_qty as trans_amount                
            from nmedwprd_db.pdwdm.resa_sales_fact s
            INNER JOIN nmedwprd_db.pdwdm.cmd_customer_dim c ON (s.cmd_customer_dim_key = c.cmd_customer_dim_key)
            INNER JOIN nmedwprd_db.pdwdm.location_dim l ON (s.Location_dim_key = l.Location_dim_key)
            INNER JOIN nmedwprd_db.cmd.customer_t cet ON (c.curr_customer_id = cet.cmd_customer_id)
            where 
            (
            (s.hdr_business_date between '{0}' and '{1}')
            and ((l.area_id = 14) or (l.region_id=201) or (l.district_id in (13011, 13021, 13014)))
            and(s.itm_dept_id not in (428, 431, 432, 441, 604, 836, 838, 904, 937, 965, 995, 822, 833, 840, 901))
            and(s.itm_item_status != 'V')
            and (c.curr_customer_id !='0')
            )
    ) X
    group by hdr_business_date""".format(*parameters)
    
    #resa_df=pd.DataFrame(scur.fetchall(), columns=['date','revenue_resa'])
    resa_df = t.read_data_from_snow_flake(query)
    resa_df = resa_df.sort_values(by='date').reset_index(drop=True)
    return resa_df


def identifying_chain_TBD():
    
    txt = """
    
   l  is pdwdm.location_dim
 
CASE WHEN (l.chain_id = 4) THEN 'LC'::varchar(2)
WHEN (l.district_id IN (13012, 13022)) THEN 'HC'::varchar(2)
WHEN (l.district_id = 12031) THEN 'CU'::varchar(2)
WHEN (l.chain_id = 2) THEN 'BG'::varchar(2)
WHEN (l.district_id = 13013) THEN 'BG'::varchar(2)
WHEN (l.district_id IN (13011, 13021, 13014)) THEN 'NM'::varchar(2)
WHEN (l.area_id = 14) THEN 'NM'::varchar(2) WHEN (l.region_id = 201) THEN 'NM'::varchar(2)
ELSE 'Other'::varchar(5) END AS Brand,

"""
    
    return


def item_return_function_TBD():
    txt = """
    itm_ref_no8 (in pdwdm.resa_sales_fact) that shows original purchase information for a return.  It does need to be parsed out, here is some sample code that does it:
 
/* Pull Returns */
proc sql;
create table fy18_returns as
select
hdr_store_id as return_location,
hdr_business_date as return_date,
itm_unit_net_purchase_amt as ret_amount,
itm_qty as ret_qty,
itm_ref_no8
from pdwdm.resa_sales_fact
where
itm_item_status = "R"
and hdr_business_date >= "30JUL17"d
and hdr_business_date <= "24NOV18"d;
quit;
 
data fy18_returns_2;
set fy18_returns;
total_ret_amount = ret_amount * ret_qty;
sale_store = input(scan(itm_ref_no8,1,"/"),4.);
if round(sale_store/1000) =1;
sale_register = input(scan(itm_ref_no8,2,"/"),6.);
sale_tran_no = scan(itm_ref_no8,3,"/");
sale_tran_number = substr(sale_tran_no, 4,4);
sale_hdr_tran_no= input(sale_tran_number, 4.);
if sale_hdr_tran_no ~="0";
sale_date = scan(itm_ref_no8,4,"/");
sale_dt = input(sale_date, 8.);
sale_d = input(put(sale_dt, 8.), MMDDYY8.);
format sale_d MMDDYY8.;
run;
 
    """
    
    return 


    



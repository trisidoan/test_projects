#!/usr/bin/env python
# coding: utf-8

### This is a data input and output utility script



import pandas as pd
import numpy as np

def sf_connection():
    import sys
    p = '/home/ec2-user/SageMaker/Repos/data-science'
    if p not in sys.path:
        sys.path.append(p)
   
    from nm_data_sci.common import ssm_cnx
    sf_cnx = ssm_cnx.get_snowflake_connection()
    return sf_cnx

def read_data_from_snow_flake(sql_query):
   
    sf_cnx = sf_connection()
    df = pd.read_sql(sql_query,sf_cnx)
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates()
    sf_cnx.close()
    return df

def SF_read_sql(stmt, verbose=False, include_ending_semicolon=False):
    sf_cnx = sf_connection()
    cur =  sf_cnx.cursor()
    stmts = split_stmt(stmt, include_ending_semicolon)
    for stmt in stmts:
        if verbose:
            print('Executing statement\n----------')
            print(stmt)
            print('----------\n')
        cur.execute(stmt)
    dta = cur.fetchall()
    df = pd.DataFrame(dta, columns=[i[0] for i in cur.description])
    df.columns = df.columns.str.lower()
    df = df.drop_duplicates()
    cur.close()
    sf_cnx.close()
    return df

def SF_read_sql_with_duplicate(stmt, verbose=False, include_ending_semicolon=False):
    sf_cnx = sf_connection()
    cur =  sf_cnx.cursor()
    stmts = split_stmt(stmt, include_ending_semicolon)
    for stmt in stmts:
        if verbose:
            print('Executing statement\n----------')
            print(stmt)
            print('----------\n')
        cur.execute(stmt)
    dta = cur.fetchall()
    df = pd.DataFrame(dta, columns=[i[0] for i in cur.description])
    df.columns = df.columns.str.lower()
    
    cur.close()
    sf_cnx.close()
    return df


def split_stmt(stmt, include_ending_semicolon=True):
    '''
    :type stmt: str
    :type include_ending_semicolon: bool
    '''
    import re
    patt = re.compile(r'''((?:[^;"']|"[^"]*"|'[^']*')+)''')
    stmt_list = []
    initial_parts = patt.split(stmt)
    for pt in initial_parts:
        pt = pt.strip()
        if pt in (';', ''): continue
        if include_ending_semicolon: pt += ';'
        stmt_list.append(pt)
    return stmt_list


def save_df_to_s3(df_to_save, file_nameX, save_dir_nameX, bucket_nameX):
    import boto3
    from io import StringIO
    from datetime import date

    csv_buffer = StringIO()
    df_to_save.to_csv(csv_buffer,index=False)
    
    today = date.today()
    # file_nameX = file_nameX.split('.')[0]
    # file_nameX = file_nameX+'_{}'.format(today.strftime('%Y%m%d'))+'.csv'
    boto3.resource('s3').Object(bucket_nameX, save_dir_nameX+file_nameX).put(Body=csv_buffer.getvalue())
    print ('saved in ', file_nameX)
    return True

def save_df_to_s3_in_zip_file(DF, file_nameX, save_dir_nameX, bucket_nameX , headerX = False):
    """
    Input: 
    DF: data frame, file_nameX: name of commpressed file (e.g 'status_indicator')
    bucket_nameX = 'nmg-analytics-ds-prod'
    save_dir_nameX = 'Users/SnowFlake_upload/'
    Output: compressed file in gzip format in save_dir_named
    
    This function should be used before call function to upload to snowflake, e.g:
    upload_df_from_S3_to_SF_Production_Table(database_schema_table, save_dir_nameX, file_nameX )
    """
    import boto3, gzip
    from io import BytesIO, TextIOWrapper
    
    gz_buffer = BytesIO()
    
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        DF.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False, header=headerX, sep='|')
        boto3.resource('s3').Object(bucket_nameX,save_dir_nameX+file_nameX).put(Body=gz_buffer.getvalue())
        print ('saved in ', file_nameX)
    return True


def upload_df_from_S3_to_SF_Proudction_Table(table_name, save_dir_nameX, file_nameX ):
    
    database_schema_table = 'NMEDWPRD_DB.MLDM.'+ table_name

    SF_read_sql("""DELETE FROM {0};
                   copy into {0}
                        from @NMEDWPRD_DB.PUBLIC.AWS_DS_PRD_STG/{1}{2}
                        file_format = (type = csv field_delimiter = '|') on_error = 'CONTINUE'
                        """.format(database_schema_table, save_dir_nameX, file_nameX))
    print("Uploaded data to the production table")
    return True



def upload_df_from_SageMaker_to_SF_Sandbox(DF,  save_flag = False, table_name = 'temp'):
    bucket_nameX = 'nmg-analytics-ds-prod'
    save_dir_nameX = 'Users/SnowFlake_upload/'
    file_nameX = table_name +'_file'
    
    column_names = ', '.join(str(c) +' VARCHAR(50)' for c in DF.columns)
    
    save_flag = save_df_to_s3_in_zip_file(DF, file_nameX, save_dir_nameX, bucket_nameX)
    if save_flag == True:
        upload_df_from_S3_to_SF_Sandbox(column_names, table_name, save_dir_nameX,file_nameX )
    return True

def upload_df_from_S3_to_SF_Sandbox(column_names, table_name, save_dir_nameX,file_nameX ):

    SF_read_sql("""drop table if exists nmedwprd_db.mktsand.{0};
                    create table nmedwprd_db.mktsand.{0} ({1});
                    copy into nmedwprd_db.mktsand.{0}
                        from @NMEDWPRD_DB.PUBLIC.AWS_DS_PRD_STG/{2}{3} 
                        file_format = (type = csv field_delimiter = '|') on_error = 'CONTINUE'
                        """.format(table_name, column_names,save_dir_nameX, file_nameX)
               )

    print("Data is uploaded!")
    return True



def upload_df_from_SageMaker_to_SnowFlake(DF, column_name, table_name = 'sk_temp'):
    SF_read_sql("""
                drop table if exists nmedwprd_db.mktsand.{0};
                create table mktsand.{0} ({1})
                """.format(table_name, column_names))
  
    sf_cnx = sf_connection()
    scur= sf_cnx.cursor()
    DF.to_sql("nmedwprd_db.mktsand.{0}".format(table_name), con=sf_cnx, index=False) #make sure index is False, Snowflake doesnt accept indexes
    scur.close()
    sf_cnx.close()
    
    
    return True

def save_df_to_s3_parquet(df, save_dir_nameX, bucket_nameX):
    import pyarrow.parquet as pq
    import s3fs
    s3 = s3fs.S3FileSystem()
    
    df.to_parquet("s3a://"+bucket_nameX+'/'+save_dir_nameX,index=True)
    #df.to_parquet(self, fname, engine='auto', compression='snappy', index=None, partition_cols=None, **kwargs)
    #s3_url = 's3://bucket/folder/bucket.parquet.gzip'
    #df.to_parquet(s3_url, compression='gzip')
    print("file is saved")
    return

def read_df_from_s3_parquet(df_to_save, save_dir_nameX, bucket_nameX):
    import pyarrow.parquet as pq
    import s3fs
    s3 = s3fs.S3FileSystem()
    
    df = pq.ParquetDataset("s3a://"+bucket_nameX+'/'+save_dir_nameX, filesystem=s3).read_pandas().to_pandas()
    return df

def read_a_file_from_s3(file_nameX, read_dir_nameX, bucket_nameX = 'nmg-analytics-ds-prod', column_namesX = False):
    import boto3
    from io import StringIO

    obj = boto3.client('s3').get_object(Bucket= bucket_nameX, Key= read_dir_nameX+file_nameX)
    
    if column_namesX == False:
        dfX = pd.read_csv(obj['Body'])
    else:
        #  usecols  = [4,6, 29,30,31,32,33]
        dfX = pd.read_csv(obj['Body'],usecols  = column_namesX)
    
    dfX.columns = dfX.columns.str.lower()
    return dfX


def contents_of_the_bucket(bucket = 'nmg-analytics-ds-prod'):
    import boto3
    
    for key in boto3.client('s3').list_objects(Bucket=bucket)['Contents']:
        print(key['Key'])
    return True

def contents_of_a_dir_in_a_bucket(prefix, bucket = 'nmg-analytics-ds-prod'):
    import boto3
    
    my_bucket = boto3.resource('s3').Bucket('nmg-analytics-ds-prod')
    
    for my_bucket_object in my_bucket.objects.filter(Prefix = prefix):
        print(my_bucket_object)
        
    return True

def read_spark_saved_data_from_S3(prefix_name, kernel_type = 'Spark_Magic'):
   # an example of a prefix_name = 'ds/dev/rec_engine/nm/output/implicit_rating/'
    
    bucket =resource.Bucket('nmg-analytics-ds-prod')
    objs = bucket.objects.filter(Prefix=prefix_name)
    
    for obj in objs:
        if '.csv' in obj.key:
            f=obj.key
    if kernel_type ==  'Spark_Magic':
        df = spark.read.csv('s3://nmg-analytics-ds-prod/{0}'.format(f))
    else:
        df = pd.read_csv('s3://nmg-analytics-ds-prod/{0}'.format(f),header=None,sep='|')
    return df

def save_spark_data_to_S3(df, prefix_name):
    # an example of a prefix_name = 'ds/dev/rec_engine/nm/output/implicit_rating/'
    df.coalesce(1).write.csv('s3://nmg-analytics-ds-prod/{0}'.format(prefix_name),mode='overwrite')
    return
                             

def move_file_dirA_dirB(file_name, dir_A, bucket_A, dir_B, bucket_B):
    df = read_a_file_from_s3(file_name, dir_A, bucket_A)
    print(df.shape)
    print(df.head(2))
    save_df_to_s3(df, file_name, dir_B, bucket_B)
    return


def move_file_from_Win_Share_to_dirB(file_name, dir_A, dir_B, bucket_B):
   
    
    df = read_a_file_from_s3(file_name, dir_A, 'nmg-analytics-da-share-prod')
    print(df.shape)
    print(df.head(2))
    save_df_to_s3(df, file_name, dir_B, bucket_B)
    return


def delete_all_files_from_a_S3_dir(dir_nameX, bucket_nameX):
    import boto3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_nameX)
    
    objects_to_delete = []
    for obj in bucket.objects.filter(Prefix=dir_nameX):
        objects_to_delete.append({'Key': obj.key})
        
    bucket.delete_objects(
        Delete={'Objects': objects_to_delete})
    return True

def delete_a_single_file_from_a_S3_dir(file_nameX, dir_nameX, bucket_nameX):
    import boto3
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_nameX, dir_nameX+file_nameX)
    obj.delete()
    return True


# In[8]:

   
def days_since_last_purchase(df):
    from datetime import date
    df["trans_date"] = pd.to_datetime(df["trans_date"])
    df["Days_Since_LPurchase"] = pd.to_datetime(date.today()) - df["trans_date"]
    df["Days_Since_LPurchase"] = df["Days_Since_LPurchase"].dt.days #Grabbing only the days from the calculation
    df.drop_duplicates('curr_customer_id',inplace=True)
    return df


def old_remove_df_based_on_key(df_before, df_suppress, key_cols):
    
    df_before = df_before.drop_duplicates()
    df_suppress = df_suppress.drop_duplicates()
    
    #N1 =  df_before[key_cols].nunique()[0]
    before_df_cust = df_before[key_cols]
    N2 = pd.merge(before_df_cust, df_suppress, on = key_cols, how='inner')[key_cols].nunique()[0]
    
    df_before = df_before.set_index(key_cols)
    df_suppress = df_suppress.set_index(key_cols)
    suppress_index_list = df_suppress.index.to_list()
    df_after = df_before.drop(suppress_index_list, errors = 'ignore').reset_index()
    
    #N3 = df_after[key_cols].nunique()[0]
    #print("Before: {:,} Excluded: {:,} After: {:,} ".format(N1, N2, N3 ))
    
    return df_after



def remove_df_based_on_key(df_before,remove_key_cols):
    
    # Removing 1M and 1M+ Spent in L12M customer list from CC
    keys = list(remove_key_cols.columns.values)
    i1 = df_before.set_index(keys).index
    i2 = remove_key_cols.set_index(keys).index
    df_after = df_before[~i1.isin(i2)].reset_index(drop=True)
    
    return df_after


def apply_customer_exclusion(df_before):
    
    sf_cnx = sf_connection()
    scur= sf_cnx.cursor()
    print("Customers before  : ", df_before['curr_customer_id'].nunique())
    
    scur.execute(""" select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                  where substr(SUPPRESSION_STRING,42,1) 
                        or substr(SUPPRESSION_STRING,1,1)
                        or substr(SUPPRESSION_STRING,3,1)
                        or substr(SUPPRESSION_STRING,5,1)
                        or substr(SUPPRESSION_STRING,6,1)
                        or substr(SUPPRESSION_STRING,7,1)""")
    suppress_df=pd.DataFrame(scur.fetchall())
    suppress_df.columns = ['curr_customer_id']
    
    print('Excluded customers: ', suppress_df['curr_customer_id'].nunique())
    
    df_after = t.remove_df_based_on_key(df_before, suppress_df,['curr_customer_id'])
    
    print("Customers after   : ", df_after['curr_customer_id'].nunique())
    scur.close()
    sf_cnx.close()
    return df_after

def order_cluster(cluster_field_name, target_field_name,df,ascending):
    ## Rearrange the cluster label in order of highest value to lower value
    new_cluster_field_name = 'new_' + cluster_field_name
    df_new = df.groupby(cluster_field_name)[target_field_name].mean().reset_index()
   
    df_new = df_new.sort_values(by=target_field_name,ascending=ascending).reset_index(drop=True)
    df_new['index'] = df_new.index
    df_final = pd.merge(df,df_new[[cluster_field_name,'index']], on=cluster_field_name)
    df_final = df_final.drop([cluster_field_name],axis=1)
    df_final = df_final.rename(columns={"index":cluster_field_name})
    #df_final = df_final.drop(['clusters'],axis=1)
    return df_final

def create_time_related_features(df):
    # input dataframe should only contain customer id and transaction date
    import pandas as pd
    
    # take only customer id and trans_data portion
    df = df[['customerid','trans_date']]
    df['trans_date'] = pd.to_datetime(df['trans_date'])
    df = df.sort_values(['customerid', 'trans_date']).drop_duplicates()

    # day related
    df['Year']  = df['trans_date'].dt.year
    df['Month'] = df['trans_date'].dt.month
    df['Week']  = df['trans_date'].dt.week
    df['Day']   = df['trans_date'].dt.dayofweek # Monday 0, Sunday 6
    
    #frequency (total number of trips)
    freq = df.groupby('customerid').trans_date.count().reset_index(name='Frequency')
    df =   pd.merge(df, freq, on='customerid')
    
    #interarrival_days
    
    df['Interarrival_Days'] = df.groupby('customerid')['trans_date'].diff().dt.days.fillna(0)
    
    # Add minimum and Maximum purchase dates and 
    first_purchase = df.groupby('customerid').trans_date.min().reset_index(name='MinPurchaseDate')
    last_purchase  = df.groupby('customerid').trans_date.max().reset_index(name = 'MaxPurchaseDate')
    first_last_purchase_dates = pd.merge(last_purchase,first_purchase,on='customerid',how='left')
    first_last_purchase_dates['Active_Duration'] = (first_last_purchase_dates['MaxPurchaseDate']-first_last_purchase_dates['MinPurchaseDate']).dt.days
    df = pd.merge(df, first_last_purchase_dates, on='customerid', how='inner')
    
    #Recency from today
    df ['Recency'] = (pd.Timestamp.today()- df['MaxPurchaseDate']).dt.days
    
    df = df[['customerid', 'trans_date', 'Year', 'Month', 'Week', 'Day', 'Frequency', 'Interarrival_Days', 'Recency', 'Active_Duration', 'MaxPurchaseDate', 'MinPurchaseDate']]
    return df

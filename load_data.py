import boto3
import pandas as pd
import numpy as np
import json
import psycopg2 as pg
from io import StringIO

import helpers as h

def get_objects(bucket_name):
    '''
    get all objects from s3 bucket
    '''
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)

    objects = []
    for obj in bucket.objects.all():
        if obj.key[-1]!='/':
            objects.append(obj.Object())
        else:
            pass
    return objects

def read_jsons(object_list):
    '''
    from a list of file paths reads jsons and returns a json array
    '''
    json_files = []
    for obj in object_list:
        obj_string = obj.get()['Body'].read().decode('utf-8')
        try:
            json_files.append(json.loads(obj_string))
        except Exception as e: #to deal with JSONDecodeError: Extra data: line 2 column 1
            err_file = obj_string.split('\n') #split at new line
            for x in err_file[:-1]:
                json_files.append(json.loads(x)) #append all extra info
    return json_files

def dict_to_df(raw_dict):
    '''
    flattens raw dictionary and formats columns
    returns dataframe
    '''
    flat_json = [h.flatten_dict(file) for file in raw_dict]
    df = pd.DataFrame(flat_json)
    df.columns = [h.format_column(col.replace(':','_')) for col in df.columns]
    return df

def stage_file(df, file_name, bucket):
    '''
    stage csv files in S3
    assumes bucket already exists
    '''
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())

def split_stage_files(df, nodes, bucket, folder='staging', suffix='test', f_type='.csv'):
    '''
    df = dataframe to split and stage
    nodes = number of chunks to split (should correspond to redshift nodes for efficiency)
    bucket = name of bucket with staging folder
    folder = name of staging folder in s3 bucket
    splits dataframe into chunks and stages these in folder in s3 bucket
    assumes folder is already there
    this is to take advantage of redshift parrallel processing
    returns files names
    '''
    chunks = np.array_split(df, nodes)
    chunks = np.array_split(df, nodes)
    files = h.random_file_names(suffix=suffix, length=nodes, f_type=f_type)
    files = [folder+'/'+file for file in files]
    file_df_dict = dict(zip(files,chunks))
    for file_name in file_df_dict:
        stage_file(file_df_dict[file_name], file_name, bucket)
    return files

def df_to_ddl(df, tablename):
    '''
    function to autogenerate ddl from DataFrame
    '''
    data_dtypes = df.dtypes.reset_index().rename(columns = {'index':'colname',0:'datatype'})

    # Map pandas datatypes into SQL
    data_dtypes['sql_dtype'] = data_dtypes.datatype.astype(str).map(
                            {'object':'varchar(256)',
                             'float64':'float',
                             'int64':'int8',
                             'bool':'boolean'}   )


    ddl = ", ".join([ "{colname} {sql_dtype} \n".format(**row)
                     for row in
                     data_dtypes[['colname','sql_dtype']].to_dict('records')])

    ddl_statement = """
    create table if not exists {tablename}
    (
    {ddl}
    )
    DISTSTYLE ALL
    ;

    """.format(tablename=tablename,ddl=ddl)
    return ddl_statement

def run_sql(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    try:
        results = cur.fetchall()
    except:
        results = None
    conn.commit()
    cur.close()
    return results

def create_table(conn, df, table_name):
    '''
    conn = redshift connection
    df = dataframe to base ddl off of
    tablename = name of table to be created
    '''
    sql = df_to_ddl(df, table_name)
    run_sql(conn, sql)

def copy_csv(conn, bucket, target_table, credentials, folder = 'staging'):
    '''
    copy staged csv files from folder to target_table
    '''
    sql = f'''
    COPY {target_table}
    FROM 's3://{bucket}/{folder}/'
    CREDENTIALS \'{credentials}\'
    CSV IGNOREHEADER 1;
    '''
    run_sql(conn, sql)

def main_data_pipe(bucket_name, iam_creds, redshift_conn, table_name, nodes):
    '''
    bucket_name = name of bucket to download all files from
    iam_creds = credentials needed to copy from s3 to redshift
    redshift_conn = psycopg2 connection object
    table_name = name given to table to create
    nodes = int, number of nodes in redshift cluster for efficient s3 copy
    '''
    #get_all objects from bucket
    object_list = get_objects(bucket_name)
    #read_json files from object list
    file_dict = read_jsons(object_list)
    #format and build dataframe
    df = dict_to_df(file_dict)
    #stage file in s3
    s3_files = split_stage_files(df, nodes, bucket=bucket_name)
    #create table if it exists
    create_table(redshift_conn, df, table_name)
    #copy files from staging folder to S3
    copy_csv(redshift_conn, bucket_name, table_name, iam_creds)
    #close connection
    redshift_conn.close()

###WOULD NEED TO EDIT WITH REDSHIFT CONNECTION INFO
if __name__ == '__main__':
    main_data_pipe(
    bucket_name = 'leafliink-data-interview-exercise',
    iam_creds = 'CREDENTIALS STRING',
    redshift_conn = pg.connect('CONNECTION INFO'),
    table_name = 'clicks_impressions',
    nodes = 2
    )

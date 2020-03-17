# Leaflink Data Exercise

## Load Data
- **load_data.py** is the main script for loading the json data source(s) in s3 bucket to a table Redshift.
- The main function with pipeline logic can be seen below
```python
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
```

## Notes
- The biggest bottleneck in the current implementation of the program is loading data from the s3 bucket
- If this data needed to be loaded regularly we'd want to filter what we pull from the bucket rather than pulling in all objects
- We could also use a multithreading technique if there was a lot more data to run the main_data_pipe in parallel for different batches

## Other files
- **initial_analysis.ipynb** is the notebook I used to get an initial understanding of the data within the s3 bucket
- **helpers.py** contains simple helper functions used in the notebook and load_data files

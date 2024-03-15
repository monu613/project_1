import json
import boto3
import pandas as pd
#from gensim.models import Word2Vec
from difflib import get_close_matches
from fuzzywuzzy import process
import csv
import pymysql

s3_client = boto3.client('s3')

    # Append matched columns to RDS table
    # rds_config = {
    # "host": "hackthon2024-debugkings.csc17vwdnwfj.us-east-1.rds.amazonaws.com",
    # "user": "Admin",
    # "password": "SxJ848fY3FSpq5jweUox"}

def mysqlconnect(): 
    try:
        # To connect MySQL database 
        conn = pymysql.connect( 
            host='hackthon2024-debugkings.csc17vwdnwfj.us-east-1.rds.amazonaws.com', 
            user='admin',  
            password='SxJ848fY3FSpq5jweUox',
            port=3306,
            db='debug_kings' 
        ) 

        cur = conn.cursor() 
        cur.execute("SELECT 1 FROM dual;") 
        output = cur.fetchall() 
        print(output)
        cur.close()
        conn.close()
    except Exception as e:
        print("Error:", e)


def detect_delimiter(bucket_name, file_key, sample_size=1024):
    s3 = boto3.client('s3')
    
    # Download a sample of the file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    body = response['Body']
    sample_data = body.read(sample_size)
    
    # Guess the delimiter
    dialect = csv.Sniffer().sniff(sample_data.decode('utf-8'))
    return dialect.delimiter


def read_file(file_path,bucket_name,file_name):
    # Get the file extension
    file_extension = file_path.split('.')[-1].lower()
    print(file_extension)
    # Read the file based on the file extension
    if file_extension == 'csv' or file_extension == 'txt':
        # Detect the delimiter for CSV/TXT files
        #delimiter = ','
        delimiter = detect_delimiter(bucket_name, file_name)
        print("Detected delimiter:", delimiter)
        df = pd.read_csv(file_path, delimiter=delimiter)
        df.head()
    elif file_extension == 'xls' or file_extension == 'xlsx':
        df = pd.read_excel(file_path)
    elif file_extension == 'json':
        df = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    return df



def match_columns_with_fuzzywuzzy(source_columns, target_columns, threshold=70):
    matched_columns = []

    # Iterate through target columns and find the closest match from the source
    for target_column in target_columns:
        best_match, score = process.extractOne(target_column, source_columns)
        if score >= threshold:
            matched_columns.append((target_column, best_match, score))
    return matched_columns
    
def display_matched_columns(matched_columns):
    print("Matched Columns:")
    for target_column, best_match, score in matched_columns:
        print(f"Target column: {target_column} - Best match: {best_match} - Score: {score}")

def append_matched_columns_to_rds(matched_columns, rds_config, table_name):
    connection = pymysql.connect(**rds_config)

    # Iterate over matched columns and append to RDS table
    cursor = connection.cursor()
    for target_column, best_match, score in matched_columns:
        query = f"INSERT INTO {table_name} (target_column, best_match, score) VALUES (%s, %s, %s)"
        cursor.execute(query, (target_column, best_match, score))
    connection.commit()
    cursor.close()
    connection.close()


def lambda_handler(event, context):
    # Read the filename from the event
    #file_name = event['file_name']
    file_name = 'employee.csv'
    
    # Read the file from S3 bucket
    bucket_name = 'hackathon2024-debugkings'
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    file_data = response['Body'].read().decode('utf-8')
    
    file_path = "s3://"+bucket_name+"/"+file_name
    print(file_path)
    # Process the file to generate the report
    # For demonstration, let's assume the report is just the file content
    #report = file_data
    dataframe = read_file(file_path,bucket_name,file_name)
    df=dataframe
    df.head()
  
    # Target columns
    target_columns = [
        'emp_id', 'dept_id', 'emp_name', 'age', 'salary',
        'hire_date', 'performance_rating', 'years_experience', 'education_level', 'gender'
    ]

    # Extract source columns from the DataFrame
    source_columns = df.columns.tolist()
    print(source_columns)
    # Match columns using fuzzywuzzy
    matched_columns = match_columns_with_fuzzywuzzy(source_columns, target_columns)

    display_matched_columns(matched_columns)
    
    # Append matched columns to RDS table
    # rds_config = {
    # "host": "hackthon2024-debugkings.csc17vwdnwfj.us-east-1.rds.amazonaws.com",
    # "user": "Admin",
    # "password": "SxJ848fY3FSpq5jweUox"}
    
    

    
    mysqlconnect()
    
    # table_name = "matched_columns"
    # append_matched_columns_to_rds(matched_columns, rds_config, table_name)

    # Return the report
    return {
        'report': matched_columns
    }
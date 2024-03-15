import json
import boto3
import pandas as pd
from gensim.models import Word2Vec
from difflib import get_close_matches
from fuzzywuzzy import process

s3_client = boto3.client('s3')

def read_file(file_path):
    # Get the file extension
    file_extension = file_path.split('.')[-1].lower()

    # Read the file based on the file extension
    if file_extension == 'csv' or file_extension == 'txt':
        # Detect the delimiter for CSV/TXT files
        delimiter = ','
        with open(file_path, 'r') as file:
            first_line = file.readline()
            if '\t' in first_line:
                delimiter = '\t'
        df = pd.read_csv(file_path, delimiter=delimiter)
    elif file_extension == 'xls' or file_extension == 'xlsx':
        df = pd.read_excel(file_path)
    elif file_extension == 'json':
        df = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    return df

def match_columns_with_fuzzywuzzy(source_columns, target_columns, threshold=70):
    matched_columns = {}

    # Iterate through target columns and find the closest match from the source
    for target_column in target_columns:
        best_match, score = process.extractOne(target_column, source_columns)
        if score >= threshold:
            matched_columns[target_column] = best_match

    return matched_columns

def lambda_handler(event, context):
    # Read the filename from the event
    file_name = event['file_name']
    
    # Read the file from S3 bucket
    bucket_name = 'Hackthon2024-DebugKings'
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    file_data = response['Body'].read().decode('utf-8')
    
    # Process the file to generate the report
    # For demonstration, let's assume the report is just the file content
    #report = file_data
    dataframe = read_file(file_data)
    df=dataframe

  
    # Target columns
    target_columns = [
        'emp_id', 'dept_id', 'emp_name', 'age', 'salary',
        'hire_date', 'performance_rating', 'years_experience', 'education_level', 'gender'
    ]

    # Extract source columns from the DataFrame
    source_columns = df.columns.tolist()

    # Match columns using fuzzywuzzy
    matched_columns = match_columns_with_fuzzywuzzy(source_columns, target_columns)

    # Display the matched columns
    for target_column, source_column in matched_columns.items():
        print(f"Target Column: {target_column}, Source Column: {source_column}")

        
    # Return the report
    return {
        'report': matched_columns.items()
    }
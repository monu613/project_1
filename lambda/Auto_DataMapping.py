import json
import boto3
import pandas as pd
#from gensim.models import Word2Vec
from difflib import get_close_matches
from fuzzywuzzy import process
import csv
import pymysql
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    non_matched_columns = []

    # Iterate through target columns and find the closest match from the source
    for target_column in target_columns:
        best_match, score = process.extractOne(target_column, source_columns)
        if score >= threshold:
            matched_columns.append((target_column, best_match, score))
        else:
            non_matched_columns.append((target_column, best_match, score))
    return matched_columns,non_matched_columns
    
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

## Email Methods 

def send_email_with_dataframes(df1, df2, recipient_email, sender_email):
    # Convert DataFrames to HTML tables
    html_content1 = df1.to_html(index=False)
    html_content2 = df2.to_html(index=False)

    # Create a multipart message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Data Analysis Results'
    msg['From'] = sender_email
    msg['To'] = recipient_email

    # Create HTML content
    html_body = f"""
    <html>
    <head></head>
    <body>
    <p>Here are the results of data analysis:</p>
    <p><b>First DataFrame:</b></p>
    {html_content1}
    <p><b>Second DataFrame:</b></p>
    {html_content2}
    </body>
    </html>
    """

    # Attach HTML content to the email
    msg.attach(MIMEText(html_body, 'html'))

    # Send the email
    ses_client = boto3.client('ses', region_name='us-east-1')
    try:
        response = ses_client.send_raw_email(
            Source=sender_email,
            Destinations=[recipient_email],
            RawMessage={'Data': msg.as_string()}
        )
        print("Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("Error sending email:", e)



# def send_mail_for_file_reject(message):
    

#     # Create a new SES resource and specify a region
#     ses_client = boto3.client('ses', region_name='us-east-1')

#     # Try to send the email
#     try:
#         # Provide the contents of the email.
#         response = ses_client.send_email(
#             Destination={
#                 'ToAddresses': [
#                     'mritunjay.singh@saama.com',
#                 ],
#             },
#             Message={
#                 'Body': {
#                     'Text': {
#                         'Charset': 'UTF-8',
#                         'Data': message,
#                     },
#                 },
#                 'Subject': {
#                     'Charset': 'UTF-8',
#                     'Data': 'File Analysis Results',
#                 },
#             },
#             Source='mritunjay.singh@saama.com',
#         )
#     # Display an error if something goes wrong.
#     except ClientError as e:
#         print(e.response['Error']['Message'])
#     else:
#         print("Email sent! Message ID:", response['MessageId'])


def analyze_scores(scores):
    total_rows = len(scores)
    score_above_50 = sum(score > 50 for score in scores)
    percentage_above_50 = (score_above_50 / total_rows) * 100
    return percentage_above_50 > 50



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
    matched_columns,non_matched_columns = match_columns_with_fuzzywuzzy(source_columns, target_columns)

    display_matched_columns(matched_columns)
    
    df_matched_columns = pd.DataFrame(matched_columns, columns=["Target column", "Best match", "Score"])

    df_matched_columns['Status'] = 'Matched'

    df_non_matched_columns = pd.DataFrame(non_matched_columns, columns=["Target column", "Best match", "Score"])

    df_non_matched_columns['Status'] = 'Not Matched'

    send_email_with_dataframes(df_matched_columns,df_non_matched_columns,'mritunjay.singh@saama.com','mrikumar613@gmail.com')
    


# # Check if more than 50% of the scores are above 50
#     if analyze_scores(scores_list):
#     # If yes, send email with data list
#         ##data_list = matched_columns
#         #recipient_email = 'recipient_email@example.com'
#         #sender_email = 'sender_email@example.com'
#         send_email_with_data(df_email)
#     else:
#     # If no, send a custom message
#         send_mail_for_file_reject("Less than 50% of Columns have mathch scores are above 70.<br><br> Hence file is rejected.")

    
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
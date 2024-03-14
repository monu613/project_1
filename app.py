import os
import json
import boto3
from flask import Flask, request, jsonify
from flask import Flask, render_template


app = Flask(__name__)

# Initialize the S3 client
s3 = boto3.client('s3', region_name='ap-south-1')  # Replace with your desired region

# Define allowed file extensions
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload', methods=['POST'])
def handle_upload():
    try:
        file = request.files['file']
        if file and allowed_file(file.filename):
            # Generate a unique filename
            new_filename = os.path.join('uploads', os.urandom(24).hex() + os.path.splitext(file.filename)[1])
            
            # Upload the file to S3
            s3.upload_fileobj(file, 'intial-code-bucket', new_filename)
            
            # Trigger the Lambda function
            trigger_lambda_function(new_filename)
            
            return jsonify({'message': 'File uploaded successfully!'})
        else:
            return jsonify({'error': 'Invalid file format. Allowed formats: txt, pdf, png, jpg, jpeg'})
    except Exception as e:
        return jsonify({'error': str(e)})

def trigger_lambda_function(file_name):
    lambda_client = boto3.client('lambda', region_name='us-west-2')  # Replace with your Lambda region
    response = lambda_client.invoke(
        FunctionName='your-lambda-function-name',
        InvocationType='Event',  # Use 'RequestResponse' for synchronous execution
        Payload=json.dumps({'file_name': file_name}),
    )
    return response

@app.route('/')
def index():
    return render_template('index.html')


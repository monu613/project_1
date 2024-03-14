import boto3
import uuid
from flask import Flask, redirect, url_for, request, render_template
from flask_sqlalchemy import SQLAlchemy
from botocore.exceptions import NoCredentialsError
from flask import Flask, request, jsonify, render_template
import boto3
import json


app = Flask(__name__)

# Initialize the S3 client
s3 = boto3.client('s3', region_name='ap-south-1')  # Replace with your desired region

# Define allowed file extensions
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

db = SQLAlchemy()

class File(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(100))
    bucket = db.Column(db.String(100))
    region = db.Column(db.String(100))
    url = db.Column(db.String(1000))  # New column to store the pre-signed URL

@app.route('/upload', methods=['POST'])
def handle_upload():
    try:
        file = request.files['file']
        if file and allowed_file(file.filename):
            # Generate a unique filename
            new_filename = os.path.join('uploads', os.urandom(24).hex() + os.path.splitext(file.filename)[1])
            bucket_name = "intital-code-bucket"  # Replace with your S3 bucket name
            # Upload the file to S3
            try:
                # IAM role is used here for AWS credentials
                #s3.upload_fileobj(uploaded_file, bucket_name, new_filename)
                # Generate a pre-signed URL for the uploaded file
                file_url = s3.generate_presigned_url('get_object',
                                                     Params={'Bucket': bucket_name,
                                                             'Key': new_filename},
                                                     ExpiresIn=3600)  # Link expires in 1 hour
            except NoCredentialsError:
                return "Credentials are not available for AWS S3."

            file = File( filename=new_filename,
                        bucket=bucket_name, region="ap-south-1", url=file_url)

            db.session.add(file)
            db.session.commit()

            #return redirect(url_for("index"))
            
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
    return render_template('index.html',files=files)

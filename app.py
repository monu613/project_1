import boto3
import uuid
from flask import Flask, redirect, url_for, request, render_template
from flask_sqlalchemy import SQLAlchemy
from botocore.exceptions import NoCredentialsError
from flask import Flask, request, jsonify, render_template
import boto3
import json

ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

db = SQLAlchemy()

class File(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    original_filename = db.Column(db.String(100))
    filename = db.Column(db.String(100))
    bucket = db.Column(db.String(100))
    region = db.Column(db.String(100))
    url = db.Column(db.String(1000))  # New column to store the pre-signed URL




def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///db.sqlite3"

    db.init_app(app)

    @app.route("/", methods=["GET", "POST"])
    def index():
        if request.method == "POST":
            uploaded_file = request.files["file-to-save"]
            if not allowed_file(uploaded_file.filename):
                return "{uploaded_file.filename}" + "File is not allowed!"

            new_filename = uploaded_file.filename + uuid.uuid4().hex + '.' + uploaded_file.filename.rsplit('.', 1)[1].lower()

            bucket_name = "hackathon2024-debugkings"  # Replace with your S3 bucket name
            s3 = boto3.client('s3')

            try:
                # IAM role is used here for AWS credentials
                s3.upload_fileobj(uploaded_file, bucket_name, new_filename)
                # Generate a pre-signed URL for the uploaded file
                file_url = s3.generate_presigned_url('get_object',
                                                     Params={'Bucket': bucket_name,
                                                             'Key': new_filename},
                                                     ExpiresIn=3600)  # Link expires in 1 hour
                trigger_lambda_function(new_filename)    
            except NoCredentialsError:
                return "Credentials are not available for AWS S3."

            file = File(original_filename=uploaded_file.filename, filename=new_filename,
                        bucket=bucket_name, region="us-east-1", url=file_url)

            db.session.add(file)
            db.session.commit()

            return redirect(url_for("index"))
            
            
        files = File.query.all()

        return render_template("index.html", files=files)
    
    return app


def trigger_lambda_function(file_name):
    lambda_client = boto3.client('lambda', region_name='us-east-1')  # Replace with your Lambda region
    response = lambda_client.invoke(
        FunctionName='hackathon_debugKings',
        InvocationType='Event',  # Use 'RequestResponse' for synchronous execution
        Payload=json.dumps({'file_name': file_name}),
    )
    return response

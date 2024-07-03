import zipfile
import boto3
import io

s3 = boto3.client('s3')

def unzip_s3_file(bucket, key, output_folder):
    obj = s3.get_object(Bucket=bucket, Key=key)
    zip_content = obj['Body'].read()

    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        for file_info in zip_ref.infolist():
            filename = file_info.filename
            file_content = zip_ref.read(filename)

            # Specify the output key/folder for unzipped files
            output_key = f'{output_folder}/{filename}'
            
            # Upload the unzipped file to S3
            s3.put_object(Body=file_content, Bucket=bucket, Key=output_key)

# Specify S3 bucket, input file key, and output folder
bucket_name = "tini-d-gluebucket-001"
input_file_key = "incoming_data/01_valid_invoices.zip"
output_folder = "incoming_unzip_data"

# Unzip the file into a different folder in S3
unzip_s3_file(bucket_name, input_file_key, output_folder) 
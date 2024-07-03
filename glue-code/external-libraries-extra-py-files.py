import boto3
import io
from pdf_generator.generate_pdf import create_pdf

def process_data():
    # Data to include in the PDF
    data = {'name': 'Bob', 'age': 35, 'city': 'Chicago'}
    # Use an in-memory bytes buffer
    pdf_buffer = io.BytesIO()
    
    # Generate PDF directly into the buffer
    create_pdf(data, pdf_buffer)
    pdf_buffer.seek(0)  # Reset the buffer pointer to the start
    print("PDF has been created.")

    # Initialize a boto3 client
    s3 = boto3.client('s3')

    # Define your bucket and the S3 key
    bucket_name = 'ti-author-scripts'
    object_key = 'ti-author-glue-scripts/ti-glue-pyspark-scripts-outputs/external-libraries-method2-output/output.pdf'

    # Upload the buffer content to S3
    s3.upload_fileobj(pdf_buffer, bucket_name, object_key)
    print(f"PDF has been uploaded to s3://{bucket_name}/{object_key}")

process_data()

import os
import json
import boto3
from pdf2image import convert_from_path

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # The path to the 'pdftoppm' tool
    poppler_path = "/usr/bin"
    
    # Retrieve PDF from S3
    bucket_name = event['bucket_name']
    object_key = event['object_key']
    input_pdf_path = f"/tmp/{os.path.basename(object_key)}"
    
    try:
        s3.download_file(bucket_name, object_key, input_pdf_path)
        print(f"Successfully downloaded {object_key} from S3 bucket {bucket_name} to {input_pdf_path}")
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error downloading file from S3: {e}")
        }

    # Check if the file exists
    if not os.path.exists(input_pdf_path):
        print(f"File not found: {input_pdf_path}")
        return {
            'statusCode': 404,
            'body': json.dumps(f"File not found: {input_pdf_path}")
        }
    
    # Output directory for images
    output_dir = "/tmp"
    
    try:
        # Convert PDF to images
        images = convert_from_path(input_pdf_path, output_folder=output_dir, poppler_path=poppler_path, fmt='png')
        
        # Optionally, save images to S3 or process them further
        for i, image in enumerate(images):
            image_path = f"{output_dir}/page_{i + 1}.png"
            image.save(image_path, "PNG")
            s3.upload_file(image_path, bucket_name, f"converted_images/{os.path.basename(object_key)}_page_{i + 1}.png")
        
        return {
            'statusCode': 200,
            'body': json.dumps('PDF conversion successful')
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"PDF conversion failed: {e}")
        }
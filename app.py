import os
import json
import boto3
import pdf2image
import io
import concurrent.futures
from PIL import Image
from botocore.exceptions import ClientError
import time
from concurrent.futures import ThreadPoolExecutor

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['DYNAMODB_TABLE']
DESTINATION_FOLDER = "invoking_bedrock_classification/proccesed/"

# Image processing settings
MAX_WIDTH = int(os.environ.get('MAX_WIDTH', 4096))
MAX_HEIGHT = int(os.environ.get('MAX_HEIGHT', 4096))
DPI = int(os.environ.get('DPI', 300))
MAX_SIZE_MB = float(os.environ.get('MAX_SIZE_MB', 4.5))

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))
    results = []

    try:
        start = time.time()
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_record, json.loads(r['body']).get('batch_id', '').strip())
                       for r in event['Records']]
            for f in futures:
                results.extend(f.result())
        elapsed = time.time() - start
        print(f"Total lambda execution time: {elapsed:.2f} seconds")
        return {'statusCode': 200, 'body': json.dumps(results)}
    except ClientError as e:
        print(f"ClientError: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': e.response['Error']['Message']})}



def process_record(batch_id):
    # Scan DynamoDB for open items (and matching batch_id)
    print(f"batch_id: {batch_id}")
    # filter_expr = "#s = :open"
    # expr_names = {"#s": "status"}
    # expr_vals = {":open": {"S": "open"}}
    # if batch_id:
    #     filter_expr += " AND #b = :batch_id"
    #     expr_names["#b"] = "batch_id"
    #     expr_vals[":batch_id"] = {"S": batch_id}

    resp = dynamodb.scan(
        TableName=TABLE_NAME,
        FilterExpression="#s = :open AND #b = :batch_id",
        ExpressionAttributeNames={"#s": "status", "#b": "batch_id"},
        ExpressionAttributeValues={":open": {"S": "open"}, ":batch_id": {"S": batch_id}}
    )

    # resp = dynamodb.scan(
    #     TableName=TABLE_NAME,
    #     FilterExpression=filter_expr,
    #     ExpressionAttributeNames=expr_names,
    #     ExpressionAttributeValues=expr_vals
    # )
    print(f"DynamoDB scan result for batch_id {batch_id}:", json.dumps(resp, indent=2))

    items = resp.get('Items', [])
    processed = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_pdf, item) for item in items]
        for f in futures:
            res = f.result()
            if res:
                processed.append(res)
    return processed



def process_pdf(item):
    object_key = item['obj_key']['S']
    case_id = item['case_id']['S']
    upload_timestamp = item['upload_timestamp']['N']
    if not object_key.lower().endswith('.pdf'):
        return None
    try:
        # update status to processing
        update_dynamodb_status(case_id, upload_timestamp, "processing_capture")
        # download PDF
        pdf_bytes = download_pdf_from_s3(BUCKET_NAME, object_key)
        # convert and upload images
        image_paths = convert_and_upload_images(pdf_bytes, case_id, object_key)
        # update status to processed with image paths
        update_dynamodb_status(case_id, upload_timestamp, "processed_capture", image_paths)
        print(f"Processed PDF {object_key}, uploaded {len(image_paths)} images")
        return object_key
    except Exception as e:
        print(f"Error processing {object_key}: {e}")
        return None

def download_pdf_from_s3(bucket, key):
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp['Body'].read()


def convert_and_upload_images(pdf_bytes, case_id, original_key):
    # Convert PDF bytes to PIL Images
    images = pdf2image.convert_from_bytes(pdf_bytes, dpi=DPI, fmt='jpeg')
    # Prepare args for parallel processing
    args = [(idx, img) for idx, img in enumerate(images)]
    # Process pages in parallel
    with ThreadPoolExecutor() as executor:
        results = executor.map(
            lambda p: compress_and_upload(p, case_id, original_key), args
        )
    # Filter out failed pages
    return [r for r in results if r]

def compress_and_upload(idx_img_tuple, case_id, original_key):
    idx, img = idx_img_tuple
    # Resize to max dimensions
    img.thumbnail((MAX_WIDTH, MAX_HEIGHT), Image.Resampling.LANCZOS)
    quality = 100
    # Try compressing until size <= MAX_SIZE_MB
    while quality >= 50:
        buf = io.BytesIO()
        img.convert('RGB').save(buf, format='JPEG', quality=quality, optimize=True)
        size_mb = buf.tell() / (1024*1024)
        if size_mb <= MAX_SIZE_MB:
            buf.seek(0)
            base = os.path.basename(original_key).replace('.pdf', '')
            key = f"{DESTINATION_FOLDER}{case_id}{base}_page_{idx+1}.jpg"
            # Upload to S3
            s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buf, ContentType='image/jpeg')
            print(f"Uploaded {key} (size: {size_mb:.2f} MB, quality: {quality})")
            return key
        quality -= 5
    print(f"⚠️ Page {idx+1} of {original_key} exceeds size limit.")
    return None


def update_dynamodb_status(case_id, upload_ts, new_status, image_paths=None):
    expr = "SET #s = :st"
    names = {"#s": "status"}
    vals = {":st": {"S": new_status}}
    if image_paths:
        expr += ", #ip = :paths"
        names["#ip"] = "image_paths"
        vals[":paths"] = {"L": [{"S": p} for p in image_paths]}
    dynamodb.update_item(
        TableName=TABLE_NAME,
        Key={"case_id": {"S": case_id}, "upload_timestamp": {"N": upload_ts}},
        UpdateExpression=expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=vals
    )
    print(f"DynamoDB status for {case_id} updated to {new_status}")

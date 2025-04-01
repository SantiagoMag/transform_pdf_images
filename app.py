import os
import json
import boto3
import pdf2image
import io
import concurrent.futures
from PIL import Image
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['DYNAMODB_TABLE']
DESTINATION_FOLDER = "invoking_bedrock_classification/processed/"


def lambda_handler(event, context):
    print("Event received:", json.dumps(event))
    
    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_results = [
                executor.submit(process_record, json.loads(record['body']).get('batch_id'))
                for record in event['Records']
            ]
            results = [future.result() for future in concurrent.futures.as_completed(future_results)]
        
        return {
            'statusCode': 200,
            'body': json.dumps(results)
        }

    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': e.response['Error']['Message']})
        }


def process_record(batch_id):
    """Procesa un batch de documentos de DynamoDB"""
    filter_expression = "#s = :open"
    expression_names = {"#s": "status"}
    expression_values = {":open": {"S": "open"}}

    if batch_id:
        filter_expression += " AND #b = :batch_id"
        expression_names["#b"] = "batch_id"
        expression_values[":batch_id"] = {"S": batch_id}

    response = dynamodb.scan(
        TableName=TABLE_NAME,
        FilterExpression=filter_expression,
        ExpressionAttributeNames=expression_names,
        ExpressionAttributeValues=expression_values
    )

    if 'Items' not in response:
        return {"message": "No items found matching criteria"}

    docs_processed = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_results = [
            executor.submit(process_pdf, item) for item in response['Items']
        ]
        for future in concurrent.futures.as_completed(future_results):
            result = future.result()
            if result:
                docs_processed.append(result)

    return docs_processed


def process_pdf(item):
    """Procesa un PDF desde S3, lo convierte en imágenes y actualiza DynamoDB"""
    object_key = item['obj_key']['S']
    case_id = item['case_id']['S']
    upload_timestamp = item['upload_timestamp']['N']

    if not object_key.endswith(".pdf"):
        return None

    try:
        update_dynamodb_status(case_id, upload_timestamp, "processing_capture")

        # Descargar PDF en memoria
        pdf_bytes = download_pdf_from_s3(BUCKET_NAME, object_key)

        # Convertir PDF a imágenes
        images = pdf_to_images(pdf_bytes)

        # Subir imágenes a S3 en paralelo
        image_paths = upload_images_to_s3(BUCKET_NAME, case_id, object_key, images)

        update_dynamodb_status(case_id, upload_timestamp, "processed_capture", image_paths)

        print(f"PDF {object_key} procesado y guardado en {DESTINATION_FOLDER}")
        return object_key

    except Exception as e:
        print(f"Error procesando el PDF {object_key}: {str(e)}")
        return None


def download_pdf_from_s3(bucket_name, object_key):
    """ Descarga un PDF desde S3 y lo carga en memoria como bytes """
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    return response['Body'].read()


def pdf_to_images(pdf_bytes, max_width=1024, max_height=1024):
    """ Convierte un PDF en memoria a imágenes PNG redimensionadas """
    images = pdf2image.convert_from_bytes(pdf_bytes, dpi=150, fmt='png')

    # Redimensionar imágenes usando ThreadPoolExecutor
    def resize_image(img):
        img.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)
        return img

    with concurrent.futures.ThreadPoolExecutor() as executor:
        return list(executor.map(resize_image, images))


def upload_images_to_s3(bucket_name, case_id, original_pdf_key, images):
    """ Sube imágenes a S3 en paralelo y retorna sus rutas """
    base_name = os.path.basename(original_pdf_key).replace(".pdf", "")
    image_paths = []

    def upload_image(i, img):
        buffer = io.BytesIO()
        img.save(buffer, "PNG")
        buffer.seek(0)

        destination_key = f"{DESTINATION_FOLDER}{case_id}/{base_name}_page_{i+1}.png"
        s3.put_object(Bucket=bucket_name, Key=destination_key, Body=buffer, ContentType="image/png")

        return destination_key

    with concurrent.futures.ThreadPoolExecutor() as executor:
        image_paths = list(executor.map(upload_image, range(len(images)), images))

    return image_paths


def update_dynamodb_status(case_id, upload_timestamp, new_status, image_paths=None):
    """ Actualiza el estado en DynamoDB """
    update_expression = "SET #s = :new_status"
    expression_names = {"#s": "status"}
    expression_values = {":new_status": {"S": new_status}}

    if image_paths:
        update_expression += ", #ip = :image_paths"
        expression_names["#ip"] = "image_paths"
        expression_values[":image_paths"] = {"L": [{"S": path} for path in image_paths]} 

    try:
        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={"case_id": {"S": case_id}, "upload_timestamp": {"N": upload_timestamp}},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values
        )
        print(f"Status actualizado a '{new_status}' para case_id: {case_id}")

    except ClientError as e:
        print(f"Error actualizando status en DynamoDB: {e.response['Error']['Message']}")

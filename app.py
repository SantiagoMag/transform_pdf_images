import os
import json
import boto3
import pdf2image
import tempfile
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

BUCKET_NAME = os.environ['BUCKET_NAME']
TABLE_NAME = os.environ['DYNAMODB_TABLE']
DESTINATION_FOLDER = "invoking_bedrock_classification/proccesed/" 


def lambda_handler(event, context):
    # EVENTO DE SQS
    print("Event received:", json.dumps(event))
        
    try:
        for record in event['Records']:
            batch_id = json.loads(record['body']).get('batch_id')
            #batch_id = json.loads(event['body']).get('batch_id')

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


            if 'Items' in response:

                docs_proccesed = []
                for item in response['Items']:
                    object_key =  item['obj_key']['S']
                    case_id = item['case_id']['S']

                    if object_key.endswith(".pdf"):
                        try:
                            update_dynamodb_status(item['case_id']['S'],"processing_capture")
                            
                            # Descargar el PDF desde S3
                            pdf_temp_path = download_pdf_from_s3(BUCKET_NAME, object_key)
                            
                            # Convertir PDF a imágenes
                            images = pdf_to_images(pdf_temp_path)
                            
                            # Subir imágenes a S3
                            upload_images_to_s3(BUCKET_NAME, case_id, object_key, images)
                                
                            update_dynamodb_status(item['case_id']['S'],"processed_capture")
                            
                            print(f"PDF {object_key} procesado y guardado en {DESTINATION_FOLDER}")
                            docs_proccesed.append(object_key)
                        except Exception as e:
                            print(f"Error procesando el PDF: {str(e)}")
                    else:
                        print(f"Archivo ignorado: {object_key}")

                return {
                    'statusCode': 200,
                    'body': json.dumps(docs_proccesed)  # Return the items that meet the filter criteria
                }
            else:
                return {
                    'statusCode': 404,
                    'body': json.dumps({"message": "No items found matching criteria"})
                }

    except ClientError as e:
        # Handle errors
        return {
            'statusCode': 500,
            'body': json.dumps({'error': e.response['Error']['Message']})
        }
   

def download_pdf_from_s3(bucket_name, object_key):
    """ Descarga el PDF desde S3 y lo guarda temporalmente """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    s3.download_file(bucket_name, object_key, temp_file.name)
    return temp_file.name

def pdf_to_images(pdf_path):
    """ Convierte un PDF a una lista de imágenes en formato PIL """
    poppler_path = "/usr/bin"

    return pdf2image.convert_from_path(pdf_path, dpi=300, poppler_path=poppler_path, fmt='png')


def upload_images_to_s3(bucket_name, case_id, original_pdf_key, images):
    """ Sube las imágenes generadas a S3 en el folder destino """
    base_name = os.path.basename(original_pdf_key).replace(".pdf", "")
    
    for i, img in enumerate(images):
        temp_image_path = f"/tmp/{base_name}_page_{i+1}.png"
        img.save(temp_image_path, "PNG",)

        destination_key = f"{DESTINATION_FOLDER}{case_id}{base_name}_page_{i+1}.png"
        s3.upload_file(temp_image_path, bucket_name, destination_key)
        os.remove(temp_image_path)  # Eliminar archivo temporal


def update_dynamodb_status(case_id, new_status):
    """ Actualiza el campo 'status' de un registro en DynamoDB a 'new_status' """
    try:
        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={"case_id": {"S": case_id}},
            UpdateExpression="SET #s = :new_status",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":new_status": {"S": f"{new_status}"}}
        )
        print(f"Status actualizado a '{new_status}' para case_id: {case_id}")
    except ClientError as e:
        print(f"Error actualizando status en DynamoDB: {e.response['Error']['Message']}")
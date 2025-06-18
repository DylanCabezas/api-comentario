import boto3
import uuid
import os
import json  # <- Faltaba importar json

def lambda_handler(event, context):
    # Entrada (json)
    print(event)

    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_name = os.environ["INGEST_BUCKET"]  # <- Faltaba agregar esta línea

    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)

    # Guardar en S3
    s3 = boto3.client('s3')
    s3_key = f"{tenant_id}/{uuidv1}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(comentario),
        ContentType='application/json'
    )

    # Retorno
    return {
        'statusCode': 200,
        'comentario': comentario,
        's3_key': s3_key,
        'response': response
    }

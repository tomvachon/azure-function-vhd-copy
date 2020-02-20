import logging

import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContainerClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    storage_information = {}
    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        storage_information['source_connection_string'] = req_body.get('source_connection_string')
        storage_information['destination_container'] = req_body.get('destination_container')
        storage_information['vhd_name'] = req_body.get('vhd_name')
        storage_information['destination_connection_string'] = req_body.get('destination_connection_string')
        storage_information['destination_base_uri'] = req_body.get('destination_base_uri')

    if storage_information:
        validate_status = validate_input(storage_information)
    else:
        logging.info("Failed to parse")
        logging.info(f"storage info: - source_connection string: {storage_information['source_connection_string']} - destination container: {storage_information['destination_container']} - vhd name: {storage_information['vhd_name']} - dst conn string: {storage_information['destination_connection_string']}")
        return func.HttpResponse(
            "Storage Information is empty or invalid json was passed",
            status_code=400
        )

    if validate_status['status'] == False:
        logging.info(f"Validation Failure - {validate_status['reason']}")
        return func.HttpResponse(
            f"Validation Failure - {validate_status['reason']}",
            status_code=400
        )
    else:
        copy_blob(storage_information)
        return func.HttpResponse(
            f"Copy job submitted successfully, blob metadata",
            status_code=200
        )



def validate_input(storage_information):
    status = True
    status_reason = ""

    if storage_information['source_connection_string']:
         logging.info(f"Source ConnString is {storage_information['source_connection_string']}")
    else:
        status_reason = "Missing Source Connection String"
        status = False

    if storage_information['destination_container'] and status == True:
         logging.info(f"Dst Container is {storage_information['destination_container']}")
    else:
        status_reason = "Missing Destination Container"
        status = False
    
    if storage_information['vhd_name'] and status == True:
         logging.info(f"VHD Name is {storage_information['vhd_name']}")
    else:
        status_reason = "Missing VHD Name"
        status = False
    
    if storage_information['destination_connection_string'] and status == True:
         logging.info(f"Dst ConnString is {storage_information['destination_connection_string']}")
    else:
        status_reason = "Missing Destination Connection String"
        status = False

    return {'status': status, 'reason': status_reason}

def copy_blob(storage_information):
    
    blob_service_client = BlobServiceClient(account_url=storage_information['destination_connection_string'])
    blob_client = blob_service_client.get_blob_client(container=storage_information['destination_container'], blob=storage_information['vhd_name'])
    copy_status = blob_client.start_copy_from_url(
        source_url=storage_information['source_connection_string']
    )

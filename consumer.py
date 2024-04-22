# -*- coding: utf-8 -*-
import requests

from robocorp.tasks import task
from robocorp import log, workitems
from RPA.Tables import Tables
from RPA.JSON import JSON

table = Tables()
json = JSON()


SALE_SYSTEM_API_URL = 'https://robocorp.com/inhuman-insurance-inc/sales-system-api'

@task
def consume_traffic_data():
    """
    This task consumes traffic data.
    """
    log.info('Consuming traffic data...')
    process_traffic_data()


def process_traffic_data():
    data_to_process = []
    for item in workitems.inputs:
        traffic_data = item.payload['traffic_data']
        valid = validate_traffic_data(traffic_data)
        # data_to_process.append(traffic_data) if valid else None
        if valid:
            status_code, json_resp = send_data_to_sale_system(traffic_data)
            if status_code == 200:
                item.done()
            else:
                item.fail(
                    workitems.ExceptionType.APPLICATION,
                    code="TRAFFIC_DATA_POST_FAILED",
                    message=json_resp['message'],
                )
        else:
            item.fail(
                workitems.ExceptionType.BUSINESS,
                code="INVALID_TRAFFIC_DATA",
                message=item.payload,
            )


    return data_to_process


def validate_traffic_data(data: dict):
    """
    This function validates traffic data.
    Validating that Country Code with 3 letters
    """
    return len(data['country']) == 3


def send_data_to_sale_system(data: dict):
    """
    This function sends data to sale system.
    """
    response = requests.post(SALE_SYSTEM_API_URL, json=data)
    return response.status_code, response.json()
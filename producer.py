# -*- coding: utf-8 -*-
from robocorp.tasks import task
from robocorp import log, workitems
from RPA.HTTP import HTTP
from RPA.Tables import Tables, Table
from RPA.JSON import JSON

http = HTTP()
table = Tables()
json = JSON()


TRAFFIC_DATA_FILE_PATH = 'output/traffic.json'
TRAFFIC_CSV_DATA_FILE_PATH = 'output/traffic.csv'
GENDER_KEY = 'Dim1'
BOTH_GENDER_VAL = 'BTSX'
RATE_KEY = 'NumericValue'
MAX_RATE_VAL = 5.0
COUNTRY_KEY = 'SpatialDim'
YEAR_KEY = 'TimeDim'
SALE_SYSTEM_API_URL = 'https://robocorp.com/inhuman-insurance-inc/sales-system-api'

@task
def producte_traffic_data():
    """
    This task produces a message with traffic data.
    """
    log.info('Producing traffic data...')
    download_traffic_data()
    traffic_table = read_traffic_data()
    traffic_table = filter_traffic_table(traffic_table)
    latest_data = get_latest_data_year_by_country(traffic_table)
    traffic_payloads = create_traffic_data_payloads(latest_data)
    create_traffic_workitem(traffic_payloads)

    # optimize 
    store_processed_traffic_data(traffic_table)


def download_traffic_data():
    """
    This function downloads traffic data.
    """
    log.info('Downloading traffic data...')
    http.download(
        url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json",
        target_file=TRAFFIC_DATA_FILE_PATH,
        overwrite=True,
    )


def read_traffic_data():
    """
    This function reads traffic data.
    """
    log.info('Reading traffic data...')
    data = json.load_json_from_file(TRAFFIC_DATA_FILE_PATH)
    traffice_datas = data.get('value', [])
    traffic_table = table.create_table(traffice_datas)
    return traffic_table


def filter_traffic_table(traffic_table: Table):
    """
    This function filters traffic data.
    """
    log.info('Filtering traffic data...')
    table.filter_table_by_column(traffic_table, RATE_KEY, '<', MAX_RATE_VAL)
    table.filter_table_by_column(traffic_table, GENDER_KEY, '==', BOTH_GENDER_VAL)
    table.sort_table_by_column(traffic_table, YEAR_KEY)
    return traffic_table


def get_latest_data_year_by_country(traffic_table: Table):
    """
    This function gets the latest data year by country.
    """
    log.info('Getting the latest data year by country...')
    data = table.group_table_by_column(traffic_table, COUNTRY_KEY)
    latest_data_by_country = []
    for table_group in data:
        latest_data = table.pop_table_row(table_group)
        latest_data_by_country.append(latest_data)
    return latest_data_by_country


def create_traffic_data_payloads(traffice_data: list):
    """
    This function prepares a message with traffic data.
    """
    log.info('Preparing traffic data message...')
    res = []
    for item in traffice_data:
        res.append(dict(
            country=item.get(COUNTRY_KEY, ''),
            year=item.get(YEAR_KEY, ''),
            rate=item.get(RATE_KEY, ''),
        ))
        log.info(f'Traffic data: {item}')
    return res


def create_traffic_workitem(traffic_payloads: list):
    """
    This function creates a work item with traffic data.
    """
    log.info('Creating traffic work item...')
    for payload in traffic_payloads:
        workitems.outputs.create(dict(traffic_data=payload))


def store_processed_traffic_data(traffic_table: Table):
    """
    This function stores processed traffic data.
    """
    log.info('Storing processed traffic data...')
    table.write_table_to_csv(traffic_table, path=TRAFFIC_CSV_DATA_FILE_PATH)
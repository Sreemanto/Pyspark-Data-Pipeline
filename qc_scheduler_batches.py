import argparse
import requests
from urllib.parse import urljoin
import pyspark
import numpy as np
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, DoubleType,ByteType,ArrayType,DateType, BooleanType
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
import json

spark = SparkSession\
    .builder\
    .appName("QC_Scheduler_Batches")\
    .enableHiveSupport()\
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")


def do_request(method, base_url, endpoint, data=None, token=None):
    """
    This function sends a HTTP request to the given API endpoint.

    :param method: A HTTP method such as GET, POST, PUT, etc.
    :param base_url: The base URL of the application
    :param endpoint: The desired API endpoint
    :param data: (Optional) A JSON-serializable data object
    :param token: (Optional) A bearer token obtained by authentication
    :return: The parsed JSON response sent by the server
    """
    headers = {'Content-Type': 'application/json'}

    if token is not None:
        headers.update({'Authorization': f'Bearer {token}'})

    #request = requests.request(method, urljoin(base_url, endpoint), json=data, headers=headers)
    try:
        request = requests.request(method, urljoin(base_url, endpoint), json=data, headers=headers)
        request.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        #print("Http Error   :",errh.args[2])
        print("Http Error   :", request.content)

    request.raise_for_status()
    return request.json()

def change_workspace(base_url, workspace_id, token):
    """
    This function generates a new bearer token for the specified workspace.

    :param base_url: The base URL of the application
    :param token: A bearer token for an arbitrary workspace
    :return: Bearer token belonging to the specified workspace
    """

    response = do_request('POST', base_url, '/api/auth/change-workspace', workspace_id, token)
    return response['token']

def get_bearer_token(base_url, workspace_name, username, password):
    """
    This function authenticates with the Scheduler server and returns a bearer token.

    The returned bearer token is specific to the workspace specified in the arguments.

    :param base_url: The base URL of the application
    :param workspace_name: Name of the workspace
    :param username: A valid username
    :param password: The corresponding password for the username
    :return: Bearer token belonging to the specified workspace
    """
    response = do_request('POST', base_url, '/api/auth/login', {
        'userName': username,
        'password': password
    })

    if 'currentWorkspace' not in response or response['currentWorkspace']['workspaceName'] != workspace_name:
        the_workspace = next(filter(lambda workspace: workspace['workspaceName'] == workspace_name,
                                    response['workspaces']), None)

        if the_workspace is not None:
            return change_workspace(base_url, the_workspace['workspaceId'], response['token'])
        else:
            raise RuntimeError(f'There is no workspace with name {workspace_name}')

    return response['token']

def get_list_of_products(base_url, token):
    """
    This function returns the list of analysts accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    response = do_request('GET', base_url, '/api/product/list', None, token)
    return response

def get_list_of_batches(base_url, token):
    """
    This function returns the list of analysts accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    response = do_request('GET', base_url, '/api/backlog/batch-list', None, token)
    return response

def update_batch(base_url, token, batch):
    """
    This function updates the availability of a given analyst.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :param batch: A batch to updated erver
    :return:
    """

    #print(batch)
    response = do_request('PUT', base_url, 'api/backlog/batch/update', {
        **batch,
    }, token)

    return response

def create_batch(base_url, token, batch):
    """
    This function updates the availability of a given analyst.
    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :param analyst: An existing analyst object received from the server
    :return:
    """
    #print(batch)
    response = do_request('POST', base_url, 'api/backlog/batch/create', {
    **batch,

    }, token)

    return response

def generate_json(requests,production_type_id):
    """
    :param requests: A dictionary containg batch details

    :returns: A dictionary which will be used as request for the create and update API
    """

    if requests:
        batch_list = []
        for batch in requests:
            batch_list.append(
                                  { "productId"            :batch["productId"],
                                    "productionTypeId"     :production_type_id,
                                    "extTasks"             : [],
                                    "extSchedules"         : [],
                                    "extCanModifyProduct"  : True,
                                    "extCanModifyProductionType": True,
                                    "id"                   : str(batch['id']),
                                    "batchUniqueIdentifier":batch["batchUniqueIdentifier"],
                                    "arrivalDate"          :batch["arrivalDate"],
                                    "deadline"             :batch["deadline"],
                                    "dueWeek"              : "",
                                    "order"                : batch['order'],
                                    "status"               : 0,
                                    "extProductName"       : "",
                                    "extProductMaterialNumber": "",
                                    "extProductionTypeName": "",
                                    "extComment"           :str(batch["SAMPLE_NUMBER"])
                                   }
                            )
        return batch_list

    else:
        return requests

def create_or_update(batch_data,batches_from_qc_df,products_from_qc_df,production_type_id):

    """
    :param batch_data: Dataframe containing batch details
    :param Raw_batch_Batches_QC: Dataframe containing details of batches that are present in the QC Tool
    :param Raw_batch_Products_QC: Dataframe containing details of products that are present in the QC Tool

    :return: Two Lists, both having batch details one for updating the other for creating batches

    """


    batches_in_qc = [row['batchUniqueIdentifier'] for row in batches_from_qc_df.select('batchUniqueIdentifier').collect()]
    print('Printing 1st few batches in QC /n',batches_in_qc[:3])
    #batch_data.show(3)

    uniquelist = batch_data.groupby('batchUniqueIdentifier','materialNumber','arrivalDate','deadline').agg(F.collect_set('sample_number')).collect()

    create_requests = []
    update_requests = []

    for batchUniqueIdentifier, materialNumber,arrivalDate, deadline, SAMPLE_NUMBER in uniquelist:

        #checking if the Product is present in the QC Tool
        if products_from_qc_df.filter(products_from_qc_df.materialNumber == materialNumber).count() > 0:

                productId = products_from_qc_df.filter(products_from_qc_df.materialNumber == materialNumber).select('id').collect()[0]['id']
                print('batchUniqueIdentifier ---->  ',batchUniqueIdentifier,'    product id : ',productId)
                print('Product Present in QC.')

                batch =     {
                            'batchUniqueIdentifier':batchUniqueIdentifier,
                            'productId'            :productId,
                            'arrivalDate'          :arrivalDate,
                            'deadline'             :deadline,
                            'SAMPLE_NUMBER'        :SAMPLE_NUMBER,
                            'order'                :0,
                            'id'                   :0
                            }

                #Checking if batch needs to be updated or created
                if batchUniqueIdentifier in batches_in_qc:
                    order =  batches_from_qc_df.filter(batches_from_qc_df.batchUniqueIdentifier == batchUniqueIdentifier).select('order').collect()[0]['order']
                    id =  batches_from_qc_df.filter(batches_from_qc_df.batchUniqueIdentifier == batchUniqueIdentifier).select('id').collect()[0]['id']
                    print('Update')
                    print('Batch_ID : ',id)
                    print('Order : ',order)

                    batch['order'] = order
                    batch['id'] = id
                    update_requests.append(batch)
                else:
                    print('Create Batch')
                    create_requests.append(batch)

        else:
            print('Product Not Present in QC. Batch cannot be Updated/Created')

    create_batch = generate_json(create_requests,production_type_id)
    update_batch = generate_json(update_requests,production_type_id)

    return update_batch,create_batch

def transform_batch(batch_data,Raw_batch_Batches_QC,Raw_batch_Products_QC,production_type_id):
    """
    :param batch_data: Dataframe containing batch details
    :param Raw_batch_Batches_QC: Dataframe containing details of batches that are present in the QC Tool
    :param Raw_batch_Products_QC: Dataframe containing details of products that are present in the QC Tool

    :return: Two Lists, both having batch details one for updating the other for creating batches
    """

    #deadline will be 2 days for runner products
    batch_data = batch_data.withColumn('deadline',F.date_add(batch_data['arrivalDate'], 4))
    batch_data = batch_data.withColumn('materialNumber',F.concat(F.col('product_code'), F.lit("_"), F.col('l_product')))
    batch_data = batch_data.withColumn('batchUniqueIdentifier',F.concat(F.col('batch_no'), F.lit("_"), F.col('inspection_lot_number'),F.lit("_"), F.col('lot_number')))

    batch_data = batch_data.select('batchUniqueIdentifier','materialNumber','arrivalDate','deadline','sample_number')

    batch_data = batch_data.withColumn("arrivalDate",batch_data["arrivalDate"].cast(StringType()))
    batch_data = batch_data.withColumn("deadline",batch_data["deadline"].cast(StringType()))

    update_batch,create_batch = create_or_update(batch_data,Raw_batch_Batches_QC,Raw_batch_Products_QC,production_type_id)
    return update_batch,create_batch

def prepare_batch(lot,Sample,static_data,runner):
    """
    :param lot: Dataframe containing Lot details coming from LIMS
    :param Sample: Dataframe containing Sample details coming from LIMS
    :param static_data: Dataframe containing details of static_data coming from LIMS
    :param runner: List of runner molecules to be filtered
    :return: A dataframe containing batch details
    """

    lot = lot.join(runner,['product_code','l_product'],'inner')
    print('Total Lot Records For runner products : ',lot.count())

    lot = lot.withColumn("lot_creation_date",lot["lot_creation_date"].cast(DateType()))
    lot = lot.where(F.datediff(F.current_date(), F.col("lot_creation_date")) < 2)
    print('Total Lot Records Before Joining : ',lot.count())


    # static_data = static_data.withColumnRenamed('p_name','l_product')\
    #         .withColumnRenamed('pg_grade','l_product_grade')\
    #         .withColumnRenamed('p_version','product_version_no')\
    #         .withColumnRenamed('pg_sampling_point','l_sampling_point')

    # static_data = static_data.withColumn("product_version_no", static_data["product_version_no"].cast(StringType()))
    # static_data = static_data.groupby(['l_product','l_product_grade','product_version_no','l_sampling_point','pg_description']).count()

    # lot = lot.join(static_data,['l_product','l_product_grade','product_version_no','l_sampling_point'],'inner')
    # print('Total Records Post Joining : ',lot.count())


    Sample = Sample.withColumn("sample_registered_on",Sample["sample_registered_on"].cast(DateType()))
    Sample = Sample.where(F.datediff(F.current_date(), F.col("sample_registered_on")) < 2)
    Sample = Sample.select('sample_number','lot_number')
    print('Total Sample Records Before Joining : ',lot.count())

    batch_data = lot.join(Sample,'lot_number','inner')
    batch_data = batch_data.withColumnRenamed('lot_creation_date','arrivalDate')

    return batch_data

def create_reporting_tables(update_requests,create_requests,batch_schema):
    """
    :param update_requests: List of batches that needs to be updated in the QC Tool
    :param create_requests: List of batches that needs to be created in the QC Tool
    :param batch_schema: Schema of the reporting tables to be created
    """

    create = spark.createDataFrame(data=create_requests,schema=batch_schema)

    create = create.withColumn("last_mod_date",F.current_timestamp())\
                .withColumn("source_detail",F.lit("DATA_FOR_QC_BATCHES_INTERFACE_CR"))\
                .withColumn("last_mod_by",F.lit("T00003222"))
    create.write.format("orc").mode('overwrite').saveAsTable('qcl.trpt_batches_create')

    update = spark.createDataFrame(data=update_requests,schema=batch_schema)

    update = update.withColumn("last_mod_date",F.current_timestamp())\
                .withColumn("source_detail",F.lit("DATA_FOR_QC_BATCHES_INTERFACE_UP"))\
                .withColumn("last_mod_by",F.lit("T00003222"))
    update.write.format("orc").mode('overwrite').saveAsTable('qcl.trpt_batches_update')

if __name__ == '__main__':

    #Reading property files
    with open("/home/appadmin/qc_scheduling/credentials.json",'r') as f:
            creds = json.load(f)
    with open("/home/appadmin/qc_scheduling/tables.json",'r') as f:
            tables = json.load(f)

    print("******")
    print("Loading parameters/credentials")
    base_url=creds["base_url"]
    workspace_name=creds["workspace_name"]
    username=creds["username"]
    password=creds["password"]
    lot=tables["lot"]
    sample=tables["sample"]
    static=tables["static"]
    runner_molecule=tables["runner_molecule"]
    production_type_id=tables["production_type_id"]
    print("******")


    token = get_bearer_token(base_url, workspace_name, username, password)

    products_from_qc = get_list_of_products(base_url, token)
    batches_from_qc = get_list_of_batches(base_url, token)

    products_from_qc_schema = StructType([
                      StructField("extCampaignGroupCount", IntegerType(), True),
                       StructField("extNoAvailableAnalyst", BooleanType(), True),
                       StructField("extTestTypeCount", IntegerType(), True),
                       StructField("id", IntegerType(), True),
                       StructField("name", StringType(), True),
                       StructField("materialNumber", StringType(), True),
                       ])



    batches_from_qc_schema = StructType([
                      StructField("id", IntegerType(), True),
                       StructField("batchUniqueIdentifier", StringType(), True),
                       StructField("order", IntegerType(), True),
                       StructField("arrivalDate", StringType(), True),
                       StructField("deadline", StringType(), True),
                       StructField("dueWeek", StringType(), True),
                       StructField("status", IntegerType(), True),
                       StructField("extProductName", StringType(), True),
                       StructField("extProductionTypeName", StringType(), True),
                       StructField("extComment", StringType(), True),
                       ])


    batches_from_qc_df = spark.createDataFrame(batches_from_qc,schema=batches_from_qc_schema)
    products_from_qc_df = spark.createDataFrame(products_from_qc,schema=products_from_qc_schema)

    #Loading Lims data
    lot = spark.table("raw.stg_labware_lot_rl").select('batch_no','inspection_lot_number','lot_number','l_description',
                                                    'l_c_market','product_code','specification_id','lot_creation_date',
                                                    'l_product','l_product_grade','l_sampling_point','product_version_no')

    static_data = spark.table("raw.stg_labware_static").select('p_name','p_version','pg_sampling_point','pg_grade','pg_description')

    runner = spark.table("raw.raw_runner_molecule").withColumnRenamed('material_code','product_code')\
                         .withColumnRenamed('specification_no','l_product')

    Sample = spark.table("raw.stg_labware_sample_rl").withColumnRenamed('s_lot','lot_number')

    batch_data = prepare_batch(lot,Sample,static_data,runner)

    update_requests,create_requests = transform_batch(batch_data,batches_from_qc_df,products_from_qc_df,production_type_id)

    print('Total No. of Batches to be created : ',len(create_requests))
    print('Total No. of Batches to be updated : ',len(update_requests))


    #Reporting Tables

    batch_schema = StructType([
        StructField('productId',IntegerType(),True),
        StructField('productionTypeId',StringType(),True),
        StructField('extTasks',ArrayType(StringType()),True),
        StructField('extSchedules',ArrayType(StringType()),True),
        StructField('extCanModifyProduct',BooleanType(),True),
        StructField('extCanModifyProductionType',BooleanType(),True),
        StructField('id',StringType(),True),
        StructField('batchUniqueIdentifier',StringType(),True),
        StructField('arrivalDate',StringType(),True),
        StructField('deadline',StringType(),True),
        StructField('order',IntegerType(),True),
        StructField('status',IntegerType(),True),
        StructField('extProductName',StringType(),True),
        StructField('extProductMaterialNumber',StringType(),True),
        StructField('extProductionTypeName',StringType(),True),
        StructField('extComment',StringType(),True)]
        )

    create_reporting_tables(update_requests,create_requests,batch_schema)

    if update_requests:
        for request in update_requests:
            print(request)
            #update_batch(base_url, token, request)

    if create_requests:
        for request in create_requests:
            print(request)
            #create_batch(base_url, token, request)

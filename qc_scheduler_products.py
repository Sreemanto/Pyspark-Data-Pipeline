import argparse
import requests
from urllib.parse import urljoin
import pyspark
import numpy as np
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, DoubleType,ByteType,ArrayType, BooleanType
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
import argparse
import json
import requests

spark = SparkSession\
    .builder\
    .appName("QC_Scheduler_Products")\
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

    request = requests.request(method, urljoin(base_url, endpoint), json=data, headers=headers)
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

def create_product(base_url,token, product):
    """
    This function creates a new entry of Products in the interface
    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :param analyst: An existing analyst object received from the server
    :return:
    """

    response = do_request('POST', base_url, '/api/product/create', {
    **product,
    }, token)

    return response


def generate_json(Products,Products_QC):
    """
    This method is used generate the json required for creating products in the QC Tool
    :param Products: Products DataFrame coming from LIMS
    :param Products_QC: Dataframe containing details of Products that already exists in QC.

    :return: A dictionary containg details of the products to be passes to the API
    """
    Products = Products.groupBy('name','materialNumber').count().select('name','materialNumber')

    dataCollect = Products.collect()
    uniqielist = []
    for row in dataCollect:
        uniqielist.append((row['name'],row['materialNumber']))

    #Generating the Json
    requests = []
    for name,materialNumber in uniqielist:

        ##Comparing if the Products is already presnt in QC or not
        if Products_QC.filter((Products_QC.name == name) & (Products_QC.materialNumber == materialNumber)).count() > 0 :
            print(name,'------',materialNumber)
        else:
            prod = {"campaignGroupInfos": [],"extCampaignGroupCount": 0,
                    "extNoAvailableAnalyst": True,"extTestTypeCount": 0,"id": 0,
                    "name": name,
                    "materialNumber": materialNumber}
            requests.append(prod)
    return requests

def transfrom_products(Products,Products_QC,static_data,runner):

    """
    This method is used to Transform the data recieved from LIMS system
    :param Products: Dataframe containing Lot details coming from LIMS
    :param Products_QC: Dataframe containing details of Products that already exists in QC.
    :param static_data : Dataframe containing Static data coming from LIMS
    :param runner : List of runner molecules to be filtered

    :return: A list of dictionary containg products infromation to be passed to the API
    """

    Products = Products.join(runner,['product_code','l_product'],'inner')
    print('Total Lot Records For runner products : ',Products.count())

    static_data = static_data.withColumnRenamed('p_name','l_product')\
                .withColumnRenamed('pg_grade','l_product_grade')\
                .withColumnRenamed('p_version','product_version_no')\
                .withColumnRenamed('pg_sampling_point','l_sampling_point')

    static_data = static_data.withColumn("product_version_no", static_data["product_version_no"].cast(StringType()))
    static_data = static_data.groupby(['l_product','l_product_grade','product_version_no','l_sampling_point','pg_description']).count()

    Products = Products.join(static_data,['l_product','l_product_grade','product_version_no','l_sampling_point'],'inner')

    Products =  Products.select('product_code','l_product','l_description','pg_description','specification_id')
    print('Total Records Post Joining : ',Products.count())


    Products = Products.withColumn('name',F.concat(F.col('l_description'),F.lit("_"),F.col('l_product'), F.lit("_"), F.col('pg_description')))
    Products = Products.withColumn('materialNumber',F.concat(F.col('product_code'), F.lit("_"), F.col('l_product')))

    Curated_Products = generate_json(Products,Products_QC)
    return Curated_Products

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
    static=tables["static"]
    runner_molecule=tables["runner_molecule"]
    print("******")


    Products = spark.table(lot).select('l_product','l_product_grade','l_sampling_point',
                                                            'product_version_no','product_code','l_c_market',
                                                            'l_description','specification_id','lot_creation_date')

    print('Total Records in Lot table : ',Products.count())

    runner = spark.table("raw.raw_runner_molecule").withColumnRenamed('material_code','product_code')\
    	      .withColumnRenamed('specification_no','l_product')
    print('Total Records in runner table : ',runner.count())

    static_data = spark.table(static).select('p_name','p_version','pg_sampling_point','pg_grade','pg_description')
    print('Total Records in Static table : ',static_data.count())

    print("Trying to connect")
    token = get_bearer_token(base_url, workspace_name, username, password)
    print("Connectivity Established")
    #Fetching the list of products from the tool
    products_from_qc = get_list_of_products(base_url, token)

    products_schema = StructType([
        StructField('campaignGroupInfos', ArrayType(IntegerType()), True),
        StructField('extCampaignGroupCount', IntegerType(), True),
        StructField('extNoAvailableAnalyst', BooleanType(), True),
        StructField('extTestTypeCount', IntegerType(), True),
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('materialNumber',StringType(), True)

    ])

    Products_QC_df = spark.createDataFrame(data=products_from_qc,schema= products_schema)


    Requests = transfrom_products(Products,Products_QC_df,static_data,runner)

    df = spark.createDataFrame(data=Requests,schema=products_schema)
    df = df.withColumn("last_mod_date",F.current_timestamp())\
              .withColumn("source_detail",F.lit("DATA_FOR_QC_PRODUCTS_INTERFACE"))\
              .withColumn("last_mod_by",F.lit("T00003222"))
    df.write.format("orc").mode('overwrite').saveAsTable('qcl.trpt_products_create')

    print('Total New Products to be Created : ',len(Requests))



    for request in Requests:
        print(request)
        create_product(base_url, token, request)

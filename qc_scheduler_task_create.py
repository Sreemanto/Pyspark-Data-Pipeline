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
from pyspark.sql import Window
from pyspark.sql.functions import first
import sys
import json

spark = SparkSession\
    .builder\
    .appName("QC_Scheduler_Task_Create")\
    .enableHiveSupport()\
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.crossJoin.enabled", "true")
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

def get_list_of_batches(base_url, token):
    """
    This function returns the list of batches accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    response = do_request('GET', base_url, '/api/backlog/batch-list', None, token)
    return response

def get_list_of_test_types(base_url, token, id):
    """
    This function returns the list of testtypes accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    #data = {"batchIds": [str(id)]}

    response = do_request('POST', base_url, 'api/task-wizard/available-options', id, token)
    return response





def get_list_of_products(base_url, token):
    """
    This function returns the list of products accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    response = do_request('GET', base_url, '/api/product/list', None, token)
    return response

def tasks_create(base_url, token, task):
    """
    This function creates a Task for a given batchUniqueIdentifier and TestType

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :param analyst: An existing analyst object received from the server
    :return:
    """
    print(task)
    response = do_request('POST', base_url, '/api/task-wizard/create-tasks', {
    **task,
    }, token)

def existing_task_check(batch_df,task_df,taskreqdf):
    """
    This function is used to check if a task is already existing in the interface

    :batch_df: A dataframe containing a deatails of all batches present in the interface.
    :task_df: A dataframe containing a deatails at Task level
    :no_copy_tasks: A dataframe containing deatails of all Task for unique batches.
    :Same_mkt_tasks: A dataframe containing a deatails at Task for duplicated batches which have the same market
    :diff_mkt_same_flag_tasks:  A dataframe containing a deatails at Task for duplicated batches which have the different market but same method of testing
    :diff_mkt_diff_flag_tasks:  A dataframe containing a deatails at Task for duplicated batches which have the different market and different method of testing
    :return: A dataframe containning details of all the existing tasks and a dataframe containing details of all the new task that will be created in the interface
    """

    t = task_df.select('existingBatchTestTypes').collect()

    if len(t)>0:
        columns = StructType([StructField('batchUniqueIdentifier',StringType(), True),
                              StructField('TestTypeId',StringType(), True),])

        existing_tasks = spark.createDataFrame([],schema=columns)
        for row in t:
            ebt = row['existingBatchTestTypes']

            batchUniqueIdentifier = []
            testTypeId = []

            for r in ebt:

                batchUniqueIdentifier.append(r['batchUniqueIdentifier'])
                testTypeId.append(r['testTypeId'])

            existing_tasks1 = spark.createDataFrame(zip(batchUniqueIdentifier,testTypeId),schema=columns)

            existing_tasks = existing_tasks.unionAll(existing_tasks1)

    existing_tasks = existing_tasks.join(batch_df,'batchUniqueIdentifier','inner').select('id','batchUniqueIdentifier','TestTypeId')

    print("Before Join ",taskreqdf.count())
    taskreqdf = taskreqdf.join(existing_tasks,'id','leftanti')
    print("After Join ",taskreqdf.count())

    return taskreqdf

def get_todays_batches(base_url, token,batch_df,prod_tt_df):

    tasks_in_qc = get_list_of_batches(base_url, token)

    task_list = []

    schema_task = StructType([StructField("batchUniqueIdentifier", StringType(), True)
                    ,StructField("arrivalDate", StringType(), True)])

    for batch in tasks_in_qc:
            task_list.append((batch['batchUniqueIdentifier'],batch['arrivalDate']))


    #Task DataFrame
    Task_qc_df = spark.createDataFrame(data = task_list,schema = schema_task)

    #Taking Today's Task only
    Task_qc_df = Task_qc_df.withColumn("arrivalDate",Task_qc_df["arrivalDate"].cast(DateType()))
    Task_qc_df_today = Task_qc_df.filter(Task_qc_df['arrivalDate']==F.current_date())

    #Today's batches
    todays_batches = [(row['batchUniqueIdentifier'].split('_')[0],row['batchUniqueIdentifier']) for row in Task_qc_df_today.select("batchUniqueIdentifier").collect()]


    todays_batches_df_schema = StructType([StructField("batch_no", StringType(), True)
                    ,StructField("batchUniqueIdentifier", StringType(), True)])
    todays_batches_df = spark.createDataFrame(data = todays_batches,schema = todays_batches_df_schema)

    #Collecting Todays BUID's
    todays_buid = [row['batchUniqueIdentifier'] for row in todays_batches_df.select(todays_batches_df["batchUniqueIdentifier"]).distinct().collect()]

    todays_batches_df = todays_batches_df.join(batch_df,['batch_no','batchUniqueIdentifier'],'inner')
    todays_batches_df = todays_batches_df.join(prod_tt_df,'productId','inner')

    todays_batches_df = todays_batches_df.withColumn('Derived_TestTypes',
          F.when(todays_batches_df['TestType'].contains("_RUN"),F.split(todays_batches_df['TestType'], '_RUN').getItem(0))\
           .when(todays_batches_df['TestType'].contains("_SETUP"),F.split(todays_batches_df['TestType'], '_SETUP').getItem(0))
           .otherwise(todays_batches_df['TestType']))

    return todays_buid, todays_batches_df

def getting_old_batch_df(todays_buid, todays_batches_df,batch_df,prod_tt_df):

    #fetching today's batches
    todays_batches = [row['batch_no'] for row in todays_batches_df.select(todays_batches_df["batch_no"]).distinct().collect()]

    df = batch_df.join(prod_tt_df,'productId','inner')

    df = df.withColumn('Derived_TestTypes',
          F.when(df['TestType'].contains("_RUN"),F.split(df['TestType'], '_RUN').getItem(0))\
          .when(df['TestType'].contains("_SETUP"),F.split(df['TestType'], '_SETUP').getItem(0))
          .otherwise(df['TestType']))

    old_batch_df = df.join(copy_flags,['specification','Derived_TestTypes','market'],'inner')


    #filterinf=g out today's buid
    old_batch_df = old_batch_df.filter(~F.col("batchUniqueIdentifier").isin(todays_buid))

    old_batches = [row['batch_no'] for row in old_batch_df.select(old_batch_df["batch_no"]).distinct().collect()]

    dup_batches = [batch for batch in todays_batches if batch in old_batches]
    new_batches = [batch for batch in todays_batches if batch not in old_batches]

    return  old_batch_df,dup_batches,new_batches

def create_task_req_df(todays_batches_df,old_batch_df,dup_batches_today,new_batches_today,copy_flags):


    todays_new_batches_df = todays_batches_df.filter(F.col("batch_no").isin(new_batches_today))

    todays_dup_batches_df = todays_batches_df.filter(F.col("batch_no").isin(dup_batches_today))
    todays_dup_batches_df = todays_dup_batches_df.join(copy_flags,['specification','Derived_TestTypes','market'],'left')
    #print(todays_dup_batches_df.show())
    print("--------------1---------------")
    #tasks compared and copy checked
    dup_task_df = todays_dup_batches_df.join(old_batch_df,['TestTypeId','copy_testing_flag'],'leftanti')
    print("--------------2---------------")
    #dup_task_df.show()
    print("--------------3---------------")
    dup_task_df = dup_task_df.select('id','batchUniqueIdentifier','TestTypeId','TestType')
    new_batches_df = todays_new_batches_df.select('id','batchUniqueIdentifier','TestTypeId','TestType')
    taskreqdf = new_batches_df.unionAll(dup_task_df)

    return taskreqdf

def prepare_test_types(task_df,products_from_qc_df):
    """
    This function creates a dataframes which has a mapping of products and it testtypes

    :task_df: A dataframe containing a deatails at Task level
    :products_from_qc_df: A dataframe conataining details of products present in the interface
    :return: A dataframes which has a mapping of products and it testtypes
    """

    p_tts = task_df.select('extProductName','productId','availableTestTypes').collect()
    columns_1 = StructType([StructField('TestTypeId',StringType(), True),
                            StructField('TestType',StringType(), True),])

    columns = StructType([StructField('TestTypeId',StringType(), True),
                          StructField('TestType',StringType(), True),
                          StructField('productId',IntegerType(), True),
                          StructField('extProductName',StringType(), True),                                ])

    prod_tt_df = spark.createDataFrame([],schema=columns)
    for p_tt in p_tts:

        TestTypeId = []
        TestType = []

        for tt in p_tt['availableTestTypes']:
            TestTypeId.append(tt['id'])
            TestType.append(tt['name'])

        prod_tt_df1 = spark.createDataFrame(zip(TestTypeId,TestType) ,schema= columns_1)
        prod_tt_df1 = prod_tt_df1.withColumn('productId',F.lit(p_tt['productId']))
        prod_tt_df1 = prod_tt_df1.withColumn('extProductName',F.lit(p_tt['extProductName']))
        prod_tt_df = prod_tt_df.unionAll(prod_tt_df1)

    prod_tt_df = prod_tt_df.select('productId','extProductName','TestTypeId','TestType')

    products_from_qc_df = products_from_qc_df.withColumn("specification", F.split(F.col("materialNumber"), "_").getItem(1))
    products_from_qc_df = products_from_qc_df.withColumn("market", F.split(F.col("name"), "_").getItem(2))
    products_from_qc_df = products_from_qc_df.select('id','specification','market').withColumnRenamed('id','productId')

    prod_tt_df = prod_tt_df.join(products_from_qc_df,'productId','inner')

    return prod_tt_df

def prepare_batch(task_df):
    """
    This function creates a dataframe which has a batch level details

    :task_df: A dataframe containing a deatails at Task level
    :return: A dataframes which has a batch level information
    """
    batch_col = task_df.select('productId','batches').collect()
    columns_1 = StructType([StructField('id',IntegerType(), True),
                            StructField('batch_no',StringType(), True),
                            StructField('batchUniqueIdentifier',StringType(), True),])

    columns = StructType([StructField('id',StringType(), True),
                          StructField('batch_no',StringType(), True),
                          StructField('batchUniqueIdentifier',StringType(), True),
                          StructField('productId',IntegerType(), True)])

    batch_df = spark.createDataFrame([],schema=columns)
    for row in batch_col:

        id = []
        batchUniqueIdentifier = []
        batch_no = []

        for batch in row['batches']:
            id.append(batch['id'])
            batchUniqueIdentifier.append(batch['batchUniqueIdentifier'])
            batch_no.append(batch['batchUniqueIdentifier'].split('_')[0])

        batch_df1 = spark.createDataFrame(zip(id,batch_no,batchUniqueIdentifier) ,schema= columns_1)
        batch_df1 = batch_df1.withColumn('productId',F.lit(row['productId']))
        batch_df1 = batch_df1.withColumn('productId',F.lit(row['productId']))
        batch_df = batch_df.unionAll(batch_df1)

    batch_df = batch_df.select('id','batch_no','batchUniqueIdentifier','productId')

    return batch_df


def generate_json(df):
    """
    This function creates a request that will be used to create a task

    :df: A dataframe containing a deatails of Task whose request needs to be created
    :return: A list that will be used to create a task
    """
    task = df.groupby('id')\
            .agg(F.collect_set('TestTypeId').alias('selectedTestTypeIds')).collect()

    task_request = []
    for row in task:

        req = {"batches" :[{'id' : row['id'],"comment":'',"selectedTestTypeIds":row['selectedTestTypeIds']}] }
        task_request.append(req)

    return task_request

def create_reporting_tables(create):
    """
    This function creates a reporting tables forcontaing the data of the request that is just sent by the DE Pipleine

    :create: A dataframe containing a deatails of Task whose request needs to be created
    :return:
    """
    create = create.withColumn("last_mod_date",F.current_timestamp())\
                .withColumn("source_detail",F.lit("DATA_FOR_QC_TASK_INTERFACE_CR"))\
                .withColumn("last_mod_by",F.lit("T00003222"))
    create.write.format("orc").mode('overwrite').saveAsTable('qcl.trpt_task_create')

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
    production_type_id=tables["production_type_id"]
    copy_flags = tables["copy_flags"]
    print("******")


    # fetches the token
    token = get_bearer_token(base_url, workspace_name, username, password)

    # fetches the batches from the interface
    batches_from_qc = get_list_of_batches(base_url, token)

    # extracting the batch ids
    batchids_qc = [batch['id'] for batch in batches_from_qc]

    # fetches the batches and its testtypes
    prod_groups = get_list_of_test_types(base_url, token, {'batchIds' :batchids_qc})

    # schema to be used to create a dataframe
    prod_groups_schema = StructType([
                                       StructField("productId", IntegerType(), True),
                                       StructField("productionTypeId", StringType(), True),
                                       StructField("extProductName", StringType(), True),
                                       StructField("extProductionTypeName", StringType(), True),
                                       StructField("batches", ArrayType(StructType([StructField("id", IntegerType(), True),
                                                                                    StructField("batchUniqueIdentifier", StringType(), True)])
                                                                                                                                              ), True),
                                       StructField("availableTestTypes", ArrayType(StructType([StructField("id", IntegerType(), True),
                                                                                    StructField("name", StringType(), True)])
                                                                                                                                              ), True),

                                       StructField("existingBatchTestTypes", ArrayType(StructType([StructField("batchUniqueIdentifier", StringType(), True),
                                                                                    StructField("testTypeId", IntegerType(), True)])
                                                                                                                                    ), True)
                                                              ])
    # fetches the produccts from the interface
    products_from_qc = get_list_of_products(base_url, token)

    # schema to be used to create a products_from_qc dataframe
    products_from_qc_schema = StructType([
                      StructField("extCampaignGroupCount", IntegerType(), True),
                       StructField("extNoAvailableAnalyst", BooleanType(), True),
                       StructField("extTestTypeCount", IntegerType(), True),
                       StructField("id", IntegerType(), True),
                       StructField("name", StringType(), True),
                       StructField("materialNumber", StringType(), True),
                       ])


    products_from_qc_df = spark.createDataFrame(products_from_qc,schema=products_from_qc_schema)

    # creating the task datframe
    task_df = spark.createDataFrame(data=prod_groups['productGroups'],schema= prod_groups_schema)

    # limiting to  the relevant tasks and where the testtypes are available
    task_df = task_df.filter(task_df.productionTypeId == production_type_id)
    task_df = task_df.filter(F.size('availableTestTypes')>0)

    # creating the prod_tt_df datframe
    prod_tt_df = prepare_test_types(task_df,products_from_qc_df)

    # creating the batch datframe
    batch_df = prepare_batch(task_df)

    # Loading the method of testing tables
    copy_flags = spark.table(copy_flags).select('tests','l_product','copy_testing_flag','market')
    copy_flags = copy_flags.withColumnRenamed("tests","Derived_TestTypes")\
                            .withColumnRenamed("l_product","specification")

    # Fetching Today's batches
    todays_buid, todays_batches_df = get_todays_batches(base_url, token,batch_df,prod_tt_df)

    # old tasks and today's duplicate batch and today's new batch
    old_batch_df,dup_batches_today,new_batches_today = getting_old_batch_df(todays_buid, todays_batches_df,batch_df,prod_tt_df)

    # creating the request dataframe
    taskreqdf = create_task_req_df(todays_batches_df,old_batch_df,dup_batches_today,new_batches_today,copy_flags)

    # existing task check
    taskreqdf = existing_task_check(batch_df,task_df,taskreqdf)

    # task request
    task_req = generate_json(taskreqdf)


    # create the reporting table
    create_reporting_tables(taskreqdf)


    # connect with the api
    if task_req:

        for request in task_request:

            print(request)
            print("----------")
            #tasks_create(base_url, token, request)

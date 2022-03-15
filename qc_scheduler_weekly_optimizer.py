import argparse
import requests
import json
from urllib.parse import urljoin
import pyspark
import datetime
import numpy as np
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, DoubleType,ByteType,ArrayType,DateType,FloatType
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext

spark = SparkSession\
    .builder\
    .appName("QC_Scheduler_Task_Create")\
    .enableHiveSupport()\
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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


    #print(data)
    if token is not None:
        headers.update({'Authorization': f'Bearer {token}'})


    try:
        print('********************************')
        request = requests.request(method, urljoin(base_url, endpoint), json=data, headers=headers)
        request.raise_for_status()

    except requests.exceptions.HTTPError as errh:

        print("Http Error   :", request.content)

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

def create_schedule(base_url, token, req):
    """
    This function returns the list of tasks accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    response = do_request('POST', base_url, '/api/schedule/create', req, token)
    return response

def pubblish_schedule(base_url, token, id):

    response = do_request('POST', base_url, '/api/schedule/publish', id, token)
    return response

def Task_qc_df(token):
    """
    This function is used to fetch the tasks from the task Interface

    :token: Token key to connect with the API
    :return: Task dataframes with tasks from the task Interface
    """
    tasks_in_qc = get_list_of_batches(base_url, token)

    task_list = []

    schema_task = StructType([StructField("taskid", IntegerType(), True)
                    ,StructField("batchUniqueIdentifier", StringType(), True)
                    ,StructField("TestType", StringType(), True)
                    ,StructField("status", StringType(), True)
                    ,StructField("deadline", StringType(), True)])

    for batch in tasks_in_qc:
        for task in batch['tasks']:
            task_list.append((task['id'],batch['batchUniqueIdentifier'],task['testTypeName'],task['status'],task['deadline']))


    Task_qc_df = spark.createDataFrame(data = task_list,schema = schema_task)

    Task_qc_df = Task_qc_df.withColumn("deadline",Task_qc_df["deadline"].cast(DateType()))

    #Filtering out the tasks whose deadline is passed
    Task_qc_df = Task_qc_df.filter(F.datediff(F.current_date(), F.col("deadline")) < 0)

    #Fetching the task which are "Completed"
    Task_qc_df = Task_qc_df.filter(Task_qc_df["status"] != 9)

    tasks = [row['taskid'] for row in Task_qc_df.select('taskid').collect()]

    return tasks

def create_schedule_request(tasks):


    dateFrom = datetime.datetime.now().strftime("%Y-%m-%d")

    dateTo = (datetime.datetime.now() + datetime.timedelta(days=5)).strftime("%Y-%m-%d")

    week = datetime.datetime.now().strftime("%W")

    req = {
                              "name": "Week "+week+" " +dateFrom+' to '+dateTo ,
                              "dateFrom": dateFrom,
                              "runOptimization": False,  ### Falswe
                              "dateTo": dateTo,
                              "taskIds": tasks
                           }

    return req

def back_end_schedule(base_url, token, data):

    response = do_request('POST', base_url, 'api/schedule/backend-scheduling', data, token)
    return response


if __name__ == '__main__':

    Reading property files
    with open("/home/datalakeqasu/qc_scheduling/credentials.json",'r') as f:
        creds = json.load(f)

    print("******")
    print("Loading parameters/credentials")
    base_url=creds["base_url"]
    workspace_name=creds["workspace_name"]
    username=creds["username"]
    password=creds["password"]
    print("******")

    token = get_bearer_token(base_url, workspace_name, username, password)
    tasks =  Task_qc_df(token)

    req = create_schedule_request(tasks)

    resp = create_schedule(base_url, token, req)
    print(resp)
    print("Schedule Created for the week")

    weekly_schedule_id = resp['id']

### add another step run the backend api scheduling

    back_end_schedule_resp = back_end_schedule(base_url, token, weekly_schedule_id)
    print("Schedule created in the Backend")

    publish_response = pubblish_schedule(base_url, token, weekly_schedule_id)
    print("Schedule Pubished for the week")

    print(publish_response)

import argparse
import requests
from urllib.parse import urljoin
import pyspark
import sys
import json
import numpy as np
import pandas as pd
from pyspark.sql.functions import first
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, DoubleType,ByteType,ArrayType,DateType,BooleanType,DateType,TimestampType
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark import SparkContext, SQLContext

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

def prepare_batch(lot,Sample,runner,NUM_DAYS):

    lot = lot.withColumn("lot_creation_date",lot["lot_creation_date"].cast(DateType()))
    lot = lot.where(F.datediff(F.current_date(), F.col("lot_creation_date")) < NUM_DAYS)


    lot = lot.join(runner,['product_code','l_product'],'inner')
    print('Total Lot Records For runner products : ',lot.count())

    Sample = Sample.withColumn("sample_registered_on",Sample["sample_registered_on"].cast(DateType()))
    Sample = Sample.where(F.datediff(F.current_date(), F.col("sample_registered_on")) < NUM_DAYS)
    Sample = Sample.select('lot_number','sample_number','test_number',
                    'analysis_common_name','analysis_batch_link','t_c_batch_name','result_number',
                            'r_c_task_group','test_submitted_date','test_status','result_status')

    batch_data = lot.join(Sample,'lot_number','inner')
    batch_data = batch_data.withColumnRenamed('lot_creation_date','arrivalDate')

    batch_data = batch_data.withColumn('materialNumber',F.concat(F.col('product_code'), F.lit("_"), F.col('l_product')))
    batch_data = batch_data.withColumn('batchUniqueIdentifier',F.concat(F.col('batch_no'), F.lit("_"), F.col('inspection_lot_number'),F.lit("_"), F.col('lot_number')))

    df = batch_data.groupby('batchUniqueIdentifier','lot_number','sample_number','test_number',
                    'analysis_common_name','analysis_batch_link','t_c_batch_name','result_number',
                            'r_c_task_group','test_submitted_date','test_status','result_status').count()


    print(df.select(df['batchUniqueIdentifier']).distinct().collect())

    print("Executed prepare_batch")

    return split_data(df,Sample)

def get_list_of_batches(base_url, token):
    """
    This function returns the list of analysts accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    response = do_request('GET', base_url, '/api/backlog/batch-list', None, token)
    return response


def map_testtypes(Task_Status,tt_map):
    """
    This function is used to map convert the old test type names to the new name

    :Task_Status: Task dataframe
    :tt_map: Table containing mapping of old and new test types
    :return: Task dataframes with new test type names
    """
    # Task_Status = Task_Status.withColumn('analysis_batch_link', F.when( (F.col('analysis_batch_link') == "Not Available"), "Blank")\
    #                      .otherwise(Task_Status['analysis_batch_link']))

    #tt_map = tt_map.withColumn('old_batch_link',F.when(tt_map['old_batch_link']=='',F.lit(None)).otherwise(tt_map['old_batch_link']))
    tt_map = tt_map.withColumnRenamed('old_common_names','analysis_common_name')\
                    .withColumnRenamed('old_batch_link','analysis_batch_link')\
                    .withColumnRenamed('new_analysis_common_names','TestType')

    tt_map = tt_map.na.fill("Not Available")


    Task_Status = Task_Status.join(tt_map,['analysis_common_name','analysis_batch_link'],'inner')
    Task_Status = Task_Status.select('batchUniqueIdentifier','TestType','status')

    return Task_Status

def split_data(df,Sample):
    """
    This function is used tio split the batch data in two dataframes

    :df: Batch Dataframe
    :Sample: Sample Tables
    :return: Two dataframe. One where t_c_batch_name is null and the other where it is not null.
    """


    df1 = df.filter(df.t_c_batch_name.isNull())
    temp = df.filter(df.t_c_batch_name.isNotNull())
    Sample = Sample.select('lot_number', 'sample_number','test_number', 'analysis_common_name',
                              'analysis_batch_link', 't_c_batch_name','result_number',
                              'r_c_task_group','test_submitted_date', 'result_status','test_status')


    temp = temp.withColumnRenamed('batchUniqueIdentifier','batchUniqueIdentifier_temp')\
                                .withColumnRenamed('lot_number','lot_number_temp')\
                                .withColumnRenamed('sample_number','sample_number_temp')\
                                .withColumnRenamed('test_number','test_number_temp')\
                                .withColumnRenamed('analysis_common_name','analysis_common_name_temp')\
                                .withColumnRenamed('analysis_batch_link','analysis_batch_link_temp')\
                                .withColumnRenamed('result_number','result_number_temp')\
                                .withColumnRenamed('r_c_task_group','r_c_task_group_temp')\
                                .withColumnRenamed('test_submitted_date','test_submitted_date_temp')\
                                .withColumnRenamed('result_status','result_status_temp')\
                                .withColumnRenamed('test_status','test_status_temp')

    df2 = temp.join(Sample,'t_c_batch_name','inner')
    df2 = df2.filter(df2.lot_number == 0).unionAll(df2.filter(df2['lot_number_temp'] == df2['lot_number']))


    df2 = df2.select('batchUniqueIdentifier_temp','lot_number','sample_number','test_number','analysis_common_name','analysis_batch_link',
                        't_c_batch_name','result_number', 'r_c_task_group','test_submitted_date','result_status', 'test_status')

    # Groupby to summarizer data at a task level
    df2 = df2.groupby(['batchUniqueIdentifier_temp','lot_number','sample_number','test_number','analysis_common_name',
                     'analysis_batch_link','t_c_batch_name','result_number','r_c_task_group','test_submitted_date']).agg({'lot_number':'mean','result_status':'first','test_status':'first'})

    df2 = df2.withColumnRenamed('batchUniqueIdentifier_temp','batchUniqueIdentifier')\
            .withColumnRenamed('first(test_status)','test_status')\
            .withColumnRenamed('first(result_status)','result_status')

    df2 = df2.select('batchUniqueIdentifier', 'test_number',
                  'analysis_common_name', 'analysis_batch_link', 't_c_batch_name', 'result_number',
                  'r_c_task_group', 'test_submitted_date', 'test_status', 'result_status').orderBy('batchUniqueIdentifier',
                                                                                'analysis_common_name','test_number','result_number')

    print("Executed split_data")

    return df1, df2


def replace_none(df):

    df = df.withColumn("r_c_task_group", F.when(F.col("r_c_task_group")=="NONE" ,F.lit(None)).otherwise(F.col("r_c_task_group")))

    return df




def restructure_data(df1,df2,buid):

        df1_ = df1[df1["batchuniqueidentifier"]==buid]
        df2_ = df2[df2["batchuniqueidentifier"]==buid]

        #picking tasklevel tests from df2
        tests = df2_.groupby(["batchuniqueidentifier",
             "analysis_common_name"])['r_c_task_group'].nunique().reset_index()
        TaskLevelTests = tests[tests["r_c_task_group"]!=0]["analysis_common_name"].to_list()

        #task level tests from df1 and including them in df2
        df2_ = pd.concat([df2_,df1_[df1_.analysis_common_name.isin(TaskLevelTests)]])

        # removing the same from df1
        df1_ = df1_[~df1_.analysis_common_name.isin(TaskLevelTests)]

        return df1_, df2_

def test_multi_status(df):
    print(df)
    l1 = df['test_status'].unique().tolist()

    if ('I' in l1) | ('P' in l1):

        return "In Progress"

    else:
        return "Completed"

def task_status(name,grp,group_task):

    grp = grp.sort_values(by="result_number")
    grp["r_c_task_group"] = grp["r_c_task_group"].replace(to_replace=np.nan, method='bfill')
    group_count = grp.r_c_task_group.nunique()
    #print(group_count)

    for i in range(group_count):
        taskname = grp.groupby("r_c_task_group").agg({"result_number":'last'}).index[i]
        last_result = grp.groupby("r_c_task_group").agg({"result_number":'last'})['result_number'].values[i]
        date = grp[grp['result_number']==last_result]["test_submitted_date"].notnull().values[0]
        task_result_status = grp[grp['result_number']==last_result]['result_status'].values[0]

        if date:
            group_task[(name[0],name[1],taskname)] = "Completed"
            #print("Completed")
        else:
            group_task[(name[0],name[1],taskname)] = task_result_status

def fetch_status(df):

    grps = df.groupby(['analysis_common_name','test_number'])
    group = {}
    group_task = {}

    for name,grp in grps:
        if grp[grp['r_c_task_group'].notnull()].shape[0] > 0:
            grp["Type"] = "Task"
            task_status(name,grp,group_task)

        else:
            grp["Type"] = "Test"
            if grp['test_status'].nunique()==1:
                group[name] = grp['test_status'].unique()[0]
            else:
                group[name] = test_multi_status(df)

    return group,group_task

def format_test_result(group):
    test = [i[0] for i in group.keys()]
    test_number = [i[1] for i in group.keys()]
    status = [i for i in group.values()]

    Test_df = pd.DataFrame([test,test_number,status]).T
    Test_df.columns = ["analysis_common_name","test_number","status"]
    Test_df = Test_df.replace(["I","P","A","X","C"],["In Progress","In Progress","Completed","Completed","Completed"])

    Test_df_final_s = Test_df.groupby(["analysis_common_name"]).agg({"status":'unique'}).reset_index()
    Test_df_final_s["status"] = ["In Progress" if "In Progress" in i else "Completed" for i in Test_df_final_s.status]

    Test_df_final_s.rename(columns = {'status':'test_status'},inplace=True)

    return Test_df_final_s

def format_task_result(group_task):

    test = [i[0] for i in group_task.keys()]
    test_number = [i[1] for i in group_task.keys()]
    task = [i[2] for i in group_task.keys()]
    status = [i for i in group_task.values()]

    Test_tasK_df = pd.DataFrame([test,test_number,task,status]).T
    Test_tasK_df.columns = ["analysis_common_name","test_number","task","status"]
    Test_tasK_df = Test_tasK_df[Test_tasK_df["task"]!="SMPL_RPT"]
    Test_tasK_df = Test_tasK_df.replace(["M","N","X","E"],["In Progress","In Progress","Completed","Completed"])

    Test_tasK_df_s = Test_tasK_df.groupby(["analysis_common_name","task"]).agg({"test_number":'last',"status":'last'}).reset_index()


    Test_tasK_df_s = Test_tasK_df_s.groupby(["analysis_common_name"]).agg({"status":'unique'}).reset_index()
    Test_tasK_df_s["status"] = ["In Progress" if "In Progress" in i else "Completed" for i in Test_tasK_df_s.status]
    Test_tasK_df_s.rename(columns = {'status':'task_status'},inplace=True)

    return Test_tasK_df_s


def merge_task_test(df,col):

    #working with sequence data
    #group -> dictionary containing final status of tests which has result_status at a task level
    group,group_task = fetch_status(df)

    #summarizing the results at single line level for each test
    Test_df_s = format_test_result(group)
    Test_task_df_s = format_task_result(group_task)

    #merging both task and test together
    df = pd.merge(Test_task_df_s,Test_df_s,on='analysis_common_name',how='outer')
    df[col] = df["task_status"].fillna(df["test_status"])
    df.drop(columns=["task_status","test_status"],inplace=True)

    return df

def fetch_start_time(df1,df2,status):

    #combining both df1 and df2 data
    df = pd.concat([df1,df2])

    #merging with the LIMS data and fetching the earliest timestamps
    temp = status.merge(df,on=["batchuniqueidentifier","analysis_common_name"], how="inner")
    temp['test_submitted_date'] = pd.to_datetime(temp['test_submitted_date'])

    #Dataframe containing only timestamp
    temp = temp.groupby(["batchuniqueidentifier","analysis_common_name"])['test_submitted_date'].min().reset_index()

    #joining back with the status dataframe
    status = pd.merge(status,temp, on =["batchuniqueidentifier","analysis_common_name"], how ='inner')

    status_map = {"Completed":9,"In Progress":5}
    status['status_map'] = status['status'].map(status_map)

    return status


def generate_json(status):

    status['test_submitted_date'] = status['test_submitted_date'].astype('str')
    status['test_submitted_date'].replace('NaT','',inplace=True)

    status_dict = zip(status['batchuniqueidentifier'].tolist(),status['status_map'].tolist(),
                       status['analysis_common_name'].tolist(),status['test_submitted_date'].tolist())

    update_req = []
    for buid,test_status,test,date in status_dict:

        if test_status == 9:
            date = ''


        update_req.append([{'batchUniqueIdentifier': buid,'status': test_status,'date': date,
                                'testTypeName': test,'stepIndex': 0}])

    return update_req

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
            task_list.append((task['id'],batch['batchUniqueIdentifier'],task['testTypeName'],task['status'],batch['deadline']))


    Task_qc_df = spark.createDataFrame(data = task_list,schema = schema_task)

    Task_qc_df = Task_qc_df.withColumn("deadline",Task_qc_df["deadline"].cast(DateType()))

    #Filtering out the tasks whose deadline is passed
    Task_qc_df = Task_qc_df.filter(F.datediff(F.current_date(), F.col("deadline")) < 0)

    #Fetching the task which are "Completed"
    Task_qc_df = Task_qc_df.filter(Task_qc_df["status"] == 5)

    tasks = Task_qc_df.toPandas()[["batchUniqueIdentifier","TestType","status"]]
    tasks.rename(columns = {"status":"qc_status"},inplace=True)

    return tasks



def get_status(df1,df2,buids,tasks):

    status = pd.DataFrame()
    for buid in buids:

        print(buid)

        df1_,df2_ = restructure_data(df1,df2,buid)

        sequence_test = merge_task_test(df2_,"seq_status")
        no_sequence_test = merge_task_test(df1_,"no_seq_status")

        #merging sequence and no sequence together to get the final status
        test_status = pd.merge(sequence_test,no_sequence_test,on='analysis_common_name',how='outer')
        test_status['status'] = test_status["seq_status"].fillna(test_status["no_seq_status"])
        test_status['batchuniqueidentifier'] = buid

        status = pd.concat([status,test_status])

        status = status[["batchuniqueidentifier","analysis_common_name","status"]].reset_index(drop=True)

    return status


def task_update(base_url, token, task):

    response = do_request('POST', base_url, '/api/schedule/set-step-statuses',task, token)

    return response



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
    runner_molecule=tables["runner_molecule"]
    production_type_id=tables["production_type_id"]
    NUM_DAYS = 4
    print("******")


    token = get_bearer_token(base_url, workspace_name, username, password)
    tasks =  Task_qc_df(token)

    if tasks.shape[0]>0:

        lot = spark.table(lot)

        # Loading the runner molecule table
        runner = spark.table(runner_molecule).withColumnRenamed('material_code','product_code')\
                                                       .withColumnRenamed('specification_no','l_product')
        # Loading the static date table
        # static_data = spark.table("raw.stg_labware_static").select('p_name','p_version','pg_sampling_point','pg_grade','pg_description')

        # Loading the sample table
        Sample = spark.table(sample).withColumnRenamed('s_lot','lot_number')

        # Loading the test type mapping table
        #tt_map =  spark.table("raw.stg_static_mapping")

        # Creating two batch datframes. One where sequence is present other where it is not
        df1, df2 = prepare_batch(lot,Sample,runner,NUM_DAYS)

        df1 = replace_none(df1)
        df2 = replace_none(df2)

        cols = ['batchuniqueidentifier','test_number', 'analysis_common_name', 'analysis_batch_link', 't_c_batch_name',
                   'result_number', 'r_c_task_group', 'test_submitted_date', 'test_status','result_status']

        df1 = df1.select(cols)
        df2 = df2.select(cols)

        df1 = df1.toPandas()
        df2 = df2.toPandas()

        buids = pd.concat([df1,df2])["batchuniqueidentifier"].unique()

        status = get_status(df1,df2,buids,tasks)

        #Merging with Task table
        status = pd.merge(status,tasks,
                  left_on=["batchuniqueidentifier","analysis_common_name"],
                            right_on = ["batchUniqueIdentifier","TestType"],
                            how = "inner")

        status = status[["batchuniqueidentifier","analysis_common_name","status","qc_status"]]

        status_time_df = fetch_start_time(df1,df2,status)
        update_req = generate_json(status_time_df)

    else:
        print("All Tasks are completed")


    for req in update_req:
        print(req)
        #task_update(base_url, token, req)
        print("---------------")

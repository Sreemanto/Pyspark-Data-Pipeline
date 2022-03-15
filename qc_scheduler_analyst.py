import argparse
import requests
from urllib.parse import urljoin
import pyspark
import numpy as np
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, DoubleType,ByteType,ArrayType,DateType
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
import json


spark = SparkSession\
    .builder\
    .appName("QC_Scheduler_Analyst")\
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
#  if data:

#    print(data)
  headers = {'Content-Type': 'application/json'}

  if token is not None:
      headers.update({'Authorization': f'Bearer {token}'})

  try:
      print('********************************')
      #print(urljoin(base_url, endpoint))
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


def get_list_of_analysts(base_url, token, refresh_type):
    """
    This function returns the list of analysts accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    if refresh_type == 'Analyst':
        response = do_request('GET', base_url, '/api/analyst/list', None, token)
        return response
    else:
        response = do_request('GET', base_url, '/api/chromatographer/list', None, token)
        return response

def update_analyst(base_url, token, analyst, availabilityExceptions, availability,refresh_type):
    """
    This function updates the availability of a given analyst.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :param analyst: An existing analyst object received from the server
    :param availability: A list of availabilities which is going to be set for the analyst
    :return:
    """
    #print(refresh_type)
    if refresh_type == 'Analyst':
        x = do_request('GET', base_url, '/api/analyst/get/'+ str(analyst['id']), None, token)['campaignGroupCapabilities']
        response = do_request('PUT', base_url, '/api/analyst/update', {
             'campaignGroupCapabilities': x ,
             'availabilityExceptions': availabilityExceptions,
             **analyst,
             'availability': []
         }, token)
    else:
        x = do_request('GET', base_url, '/api/chromatographer/get/'+ str(analyst['id']), None, token)['campaignGroupCapabilities']
        #print(x)
        response = do_request('PUT', base_url, '/api/chromatographer/update', {
             'campaignGroupCapabilities': x ,
             'availabilityExceptions': availabilityExceptions,
             **analyst,
             'availability': []
         }, token)

    return response

def create_analyst(base_url, token, analyst, refresh_type):
    """
    This function updates the availability of a given analyst.
    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :param analyst: An existing analyst object received from the server
    :return:
    """
    if refresh_type == 'Analyst':
        response = do_request('POST', base_url, '/api/analyst/create', {
        'campaignGroupCapabilities': [],
        'availabilityExceptions': [],
        **analyst,
        'availability': [],
        }, token)

        return response
    else:
        response = do_request('POST', base_url, "/api/chromatographer/create", {
        'campaignGroupCapabilities': [],
        'availabilityExceptions': [],
        "resourceType":2,
        **chromatographer,
        'availability': [],
        }, token)

        return response



def call_api(create_analysts,update_analysts,analysts_from_qc,refresh_type):
    '''
    This methode calls both the update and create api.
    :param create_analysts: Analysts to be created in the QC Tool
    :param update_analysts: Analysts whose shift schedule is to be updates in the QC Tool
    :param analysts_from_qc: json object containg all the analysts which are already existing in the tool
    '''
    if create_analysts:
        for analyst in create_analysts:
            create_analyst(base_url, token,  {'name': analyst['name']},refresh_type)
        pass
    else:
        print('No new analyst')


    if update_analysts:

        for analyst in update_analysts:

            the_analyst = next(filter(lambda analyst_: analyst_['name'] == analyst['name'], analysts_from_qc), None)
            if the_analyst:

                analyst_req = {}
                analyst_req['name'] = the_analyst['name']
                analyst_req['isActive'] = the_analyst['isActive']
                analyst_req['id'] = the_analyst['id']
                analyst_req['resourceType'] = the_analyst['resourceType']

                #print(analyst_req)

                update_analyst(base_url, token, analyst_req, analyst['availabilityExceptions'],analyst['availability'],refresh_type)


    else:
        print('No updates')


def convert_to_date(spark_df: DataFrame, cols: dict):

    """
    This function converts given a date columns into "yyyy-MM-dd HH:mm:ss" format.
    Args:
        spark_df: spark data frame
        cols: the list of column to be converted into date format.

    Returns: Returns spark data frame with given date columns into "yyyy-MM-dd HH:mm:ss" format.

    """

    for dt_column, dt_fmt in cols.items():
        spark_df = spark_df.withColumn(
            dt_column, F.col(dt_column).cast(StringType())
        ).withColumn(
            dt_column,
            F.from_unixtime(
                F.unix_timestamp(dt_column, dt_fmt), "yyyy-MM-dd HH:mm:ss"
            ).cast("timestamp"),
        )
    return spark_df


def generate_json(analysts,shift_map):

    analysts = analysts.join(shift_map,'shift_code','inner')

    request_list = []

    for row in  analysts.groupby('name_id').agg(F.collect_list(F.array("S_1", "S_2","S_3")).alias('availability')).collect():

        requests = {}

        requests['name'] = row['name_id']

        requests['availability'] = np.array(row['availability']).flatten()

        request_list.append(requests)

    return request_list


def create_or_update(Analysts,Shift_Report):
    """
    This method is used to find if the group of analysts which either
    needs to be updated or created.
    """

    Shift_Report = Shift_Report.withColumn('name_id',F.concat(F.col('empname'), F.lit("_"), F.col('empcode')))

    df_common_ananlyst = Shift_Report.join(Analysts, 'name_id', 'inner').select('name_id','shift_code','shift_date')

    df_new_analyst = Shift_Report.join(Analysts, 'name_id','leftanti').select('name_id','shift_code','shift_date')

    return df_common_ananlyst,df_new_analyst


def transfom_analyst(analysts_from_qc):
    """
    This method is used to load the json and format it
    """

    names = [row['name'].split('_')[0] for row in analysts_from_qc.select('name').collect()]
    ids = [row['name'].split('_')[1].lstrip('0') for row in analysts_from_qc.select('name').collect()]

    name_id = [name+'_'+id for name,id in zip(names,ids)]

    Analysts = spark.createDataFrame(name_id,schema=StringType()).withColumnRenamed('value','name_id')

    return Analysts




def create_availability(request_list):

    update_json_list = []

    for req in request_list:

        update_json = {}

        update_json['name'] = req['name']

        update_json['availabilityExceptions'] = [{"availableFrom":str(req['shift_date'][index]),
                                                 "availableTo":str(req['shift_date'][index]),
                                                 "value":int(req['availability'][3*index] +
                                                         req['availability'][3*index+1] +
                                                         req['availability'][3*index+2]),
                                                 "shift":int(req['shift_no'][index]),}  for index in range(len(req['shift_date']))
                                                                                   if (req['availability'][3*index] +
                                                                                       req['availability'][3*index+1] +
                                                                                       req['availability'][3*index+2]) != 0 ]



        update_json['availability'] = [{'shift':str(int(index%NUM_SHIFTS+1)),
                 'day': str(int(index/NUM_SHIFTS+1)),
                 'value': str(int(req['availability'][index]))} for index in range(len(req['availability']))]


        for i in range(-3,0):
            update_json['availability'][i]['day'] = '0'

        update_json_list.append(update_json)

    return update_json_list



def generate_upd_json(update_analysts,shift_map_df):

    update_analysts = update_analysts.join(shift_map_df,'shift_code','inner').orderBy('name_id','shift_date')
    update_analysts = update_analysts.withColumn("shift_date",update_analysts["shift_date"].cast(StringType()))

    request_list = []

    for row in  update_analysts.groupby('name_id').agg(F.collect_list(F.array("S_1", "S_2","S_3")).alias('availability'),
                                                      F.collect_list("shift_date").alias('shift_date'),
                                                      F.collect_list("shift_no").alias('shift_no')).orderBy('shift_date').collect():

        requests = {}

        requests['name']         = row['name_id']
        requests['availability'] = np.array(row['availability']).flatten()
        requests['shift_date']   = row['shift_date']
        requests['shift_no']     = row['shift_no']

        request_list.append(requests)

    req = create_availability(request_list)

    return req


def filter_data(df,plantname,department):

    df = df.filter(F.col("plantname").isin(plantname))
    df = df.filter(F.col("department").isin(department))

    df = convert_to_date(df, {'shift_date': 'dd/MM/yyyy'})

    df = df.withColumn("shift_date", F.col("shift_date").cast(DateType()))
    df = df.withColumn("Current_date", F.current_date())

    df = df.withColumn('last_date',F.date_add(df['Current_date'], NUM_DAYS))

    df = df.filter(F.col('shift_date') < F.col('last_date'))

    df = df.filter(F.col('shift_date') >= F.current_date())

    df = df.withColumn('empcode', F.regexp_replace('empcode', r'^[0]*', ''))

    print("-------------------------------------")

    return df


def get_schema():

    analysts_from_qc_schema = StructType([
                      StructField("availability", ArrayType(StructType([StructField("id", IntegerType(), True),
                                                                        StructField("resourceId", IntegerType(), True),
                                                                        StructField("day", IntegerType(), True),
                                                                        StructField("value", DoubleType(), True),
                                                                        StructField("isActive", StringType(), True),
                                                                        StructField("shift", IntegerType(), True)])), True),
                       StructField("numberOfCampaignGroupCapabilities", IntegerType(), True),
                       StructField("isActive", StringType(), True),
                       StructField("id", IntegerType(), True),
                       StructField("name", StringType(), True),
                       StructField("expiryDate", StringType(), True),
                       StructField("resourceType", IntegerType(), True),
                       StructField("extWeeklyWorkingHours", DoubleType(), True),
                       StructField("extStatus", IntegerType(), True)])



    return analysts_from_qc_schema


def shift_map():

    shift_map = [("WO",0,0,0,0),("A",7,0,0,1),("B",0,7,0,2),("C",0,0,7,3),("G",0,7,0,2)]

    shift_map_Schema = StructType([ StructField('shift_code', StringType(), True),
                                StructField('S_1', IntegerType(), True),
                                StructField('S_2', IntegerType(), True),
                                StructField('S_3', IntegerType(), True),
                                StructField('shift_no', IntegerType(), True)])


    shift_map_df = spark.createDataFrame(shift_map,shift_map_Schema)


    return shift_map_df

def create_reporting_tables(update_analysts,create_analysts,analyst_schema):

    create = spark.createDataFrame(data=create_analysts,schema=analyst_schema)
    create = create.withColumn("last_mod_date",F.current_timestamp())\
                .withColumn("source_detail",F.lit("DATA_FOR_QC_ANALYST_INTERFACE_CR"))\
                .withColumn("last_mod_by",F.lit("P90009627"))
    create.write.format("orc").mode('overwrite').saveAsTable('qcl.trpt_products_create')

    update = spark.createDataFrame(data=update_analysts,schema=analyst_schema)
    update = update.withColumn("last_mod_date",F.current_timestamp())\
                .withColumn("source_detail",F.lit("DATA_FOR_QC_ANALYST_INTERFACE_UP"))\
                .withColumn("last_mod_by",F.lit("P90009627"))
    update.write.format("orc").mode('overwrite').saveAsTable('qcl.trpt_products_update')


if __name__ == '__main__':

    #Reading property files
    with open("/home/appadmin/qc_scheduling/credentials.json",'r') as f:
        creds = json.load(f)
    with open("/home/appadmin/qc_scheduling/analyst_parameters.json",'r') as f:
        params = json.load(f)

    print("******")
    print("Loading parameters/credentials")
    base_url=creds["base_url"]
    workspace_name=creds["workspace_name"]
    username=creds["username"]
    password=creds["password"]
    NUM_SHIFTS=params["NUM_SHIFTS"]
    NUM_DAYS=params["NUM_DAYS"]
    plantname=params["plantname"]
    department=params["department"]
    resource_type = params["resource_type"]
    print("******")

    for refresh_type in resource_type:

        print(refresh_type)

        #Generate Token
        token = get_bearer_token(base_url, workspace_name, username, password)

        #Load Shift Data
        Shift_Report = spark.table("mfg.tfct_shift_schedule").select('plantname','plantcode','department','empcode',
                                    'empname','shift_date','shift_code','rundate')

        #Filter for next seven days
        Shift_Report = filter_data(Shift_Report,plantname,department)

        #Fetch analysts from qc
        analysts_from_qc = get_list_of_analysts(base_url, token, refresh_type)
        #print(analysts_from_qc)

        #Create shift_map table
        shift_map_df = shift_map()
        #print(shift_map_df.show(3))

        #Get schema and create the analyst dataframe
        analysts_from_qc_schema = get_schema()
        analysts_df = spark.createDataFrame(data=analysts_from_qc,schema= analysts_from_qc_schema)
        #print(analysts_df.show(3))

        #Transfrom analyst
        analysts_df = transfom_analyst(analysts_df)

        #Find update and create analysts
        update_analysts,create_analysts = create_or_update(analysts_df, Shift_Report)

        update_requests = generate_upd_json(update_analysts,shift_map_df)
        create_requests = generate_json(create_analysts,shift_map_df)

        analyst_schema = StructType([
                 StructField('name', StringType(), True),
                 StructField('availability', ArrayType(IntegerType()), True)])

        print('Total No. of Analysts to be created : ',len(create_requests))
        print('Total No. of Analysts to be updated : ',len(update_requests))

        call_api(create_requests, update_requests, analysts_from_qc,refresh_type)

        create_reporting_tables(update_analysts,create_analysts,analyst_schema)

        print("Refresh for {} done".format(refresh_type))

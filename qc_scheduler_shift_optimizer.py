import argparse
import json
import requests
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
        print("Base URL : ",base_url)
        print("Endpoint : ",endpoint)
        request = requests.request(method, urljoin(base_url, endpoint), json=data, headers=headers)
        request.raise_for_status()
       # print(request)

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

def get_schedule(base_url, token):

    response = do_request('GET', base_url, 'api/schedule/list', None, token)

    current_week = int((datetime.datetime.now()).strftime("%W"))
    #print(current_week)

    df = pd.DataFrame(response)[['id','dateFrom', 'dateTo','status','extWeek']]
    schedule_id =  df[(df['extWeek']==current_week) & (df['status']==1)].id.max()

    return schedule_id

def get_duplicate_schedule(base_url, token, id):

    response = do_request('POST', base_url, '/api/schedule/duplicate', id, token)
    return response

def save_task(base_url, token, id,data):

    response = do_request('POST', base_url, '/api/schedule/' + str(id) + '/save-tasks-for-schedule', data, token)
    return response

def back_end_schedule(base_url, token, data):

    response = do_request('POST', base_url, 'api/schedule/backend-scheduling', data, token)
    return response

def update_schedule_name(base_url, token, data):

    response = do_request('PUT', base_url, '/api/schedule/update', data, token)
    return response

def get_sch(base_url, token, id):
    print('/api/schedule/get/'+str(id))
    response = do_request('GET', base_url, '/api/schedule/get/'+str(id), None, token)
    return response

def publish_schedule(base_url, token, id):

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
            task_list.append((task['id'],batch['batchUniqueIdentifier'],task['testTypeName'],task['status'],batch['deadline']))


    Task_qc_df = spark.createDataFrame(data = task_list,schema = schema_task)

    Task_qc_df = Task_qc_df.withColumn("deadline",Task_qc_df["deadline"].cast(DateType()))

    #Filtering out the tasks whose deadline is passed
    Task_qc_df = Task_qc_df.filter(F.datediff(F.current_date(), F.col("deadline")) < 0)

    #iltering the task which are "Completed"/"On Hold"
    #Task_qc_df = Task_qc_df.filter(Task_qc_df['deadline'] > "2021-10-28" )
    Task_qc_df = Task_qc_df.filter(Task_qc_df["status"] != 9)
    Task_qc_df = Task_qc_df.filter(Task_qc_df["status"] != 4)

    tasks = [row['taskid'] for row in Task_qc_df.select('taskid').collect()]

    return tasks,Task_qc_df

def generate_shift_name():

    hour = int(datetime.datetime.now().strftime("%H"))
    month = datetime.datetime.now().strftime("%B")
    week = datetime.datetime.now().strftime("%W")

    if (hour < 12):
        shift = "Shift_1"
        current_shift = 1
        close_shift = 3
        close_date =  (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    elif hour > 21:
        shift = "Shift_3"
        current_shift = 3
        close_shift = 2
        close_date =  (datetime.datetime.now()).strftime("%Y-%m-%d")
    else:
        shift = "Shift_2"
        current_shift = 2
        close_shift = 1
        close_date =  (datetime.datetime.now()).strftime("%Y-%m-%d")

    return month+'_Week_'+week+'_'+shift+'_'+datetime.datetime.now().strftime("%H,%M,%S"),close_date,close_shift,current_shift

def get_step_edit(base_url, token, schedule_id,schedule_step_id):

    #print('/api/schedule/' + str(schedule_step_id) + '/get-step-for-edit/'+str(schedule_id))

    response = do_request('GET', base_url, '/api/schedule/' + str(schedule_step_id) + '/get-step-for-edit/'+str(schedule_id), None, token)

    return response

def save_step(base_url, token, req ,  id):

    response = do_request('POST', base_url, '/api/schedule/' + str(id) + '/save-step/', req, token)

    return response
#mother API
def get_schedule_dashboard(base_url, token, id):


   # print("Schedule Request : '/api/schedule/' + str(id) + '/dashboard/')
    response = do_request('GET', base_url, '/api/schedule/' + str(id) + '/dashboard/', None, token)
    return response

def create_schedule_request(tasks):
    """
    This function returns the list of tasks accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    #static dict keep a track of difference of days from Monday
    days_from_next_monday = {'Mon' : 7,"Tue" : 6,"Wed" : 5,"Thu" : 4,"Fri" : 3,"Sat" : 2,"Sun" : 1}


    Today = datetime.datetime.now().strftime("%a")
    days_diff_from_start = days_from_next_monday[Today]
    days_diff_from_end = days_diff_from_start + 6

    #second week start date
    dateFrom = (datetime.datetime.now() + datetime.timedelta(days=days_diff_from_start)).strftime("%Y-%m-%d")

    #second week end date
    dateTo = (datetime.datetime.now() + datetime.timedelta(days=days_diff_from_end)).strftime("%Y-%m-%d")

    week = int((datetime.datetime.now()).strftime("%W")) + 1

    print("Start Date : ", dateFrom)
    print("End Date : ", dateTo)
    print("Week : ", week)

    req = {"name": "Week "+str(week)+" " +dateFrom+' to '+dateTo ,
           "dateFrom": dateFrom,
           "runOptimization": False,
           "dateTo": dateTo,
           "taskIds": tasks}

    return req

def collect_stepgroups(dashboard_api_response,resource_id,current_shift):

    #fetching current days
    day = datetime.datetime.now().strftime("%a").lower()

    #fetching active analysts from the dashbaord
    analysts = list(dashboard_api_response["scheduleCalendar"]["0"].keys())

    stepgroup_id_list = []

    for analyst in analysts:
        print("Cehecking Stepgroups for {} ".format(analyst))

        #checking for all three shifts
        for shift in range(3):

            #checking if there's a stepgroup available in the current shift
            if dashboard_api_response["scheduleCalendar"]["0"][analyst]["week"][day]["shifts"][shift]["shift"] == current_shift:

                current_shift_stpgrps = dashboard_api_response["scheduleCalendar"]["0"][analyst]["week"][day]["shifts"][shift]["stepGroups"]

                if len(current_shift_stpgrps)>0:

                    #collecting the stepgroups
                    stepgroupids = [stepgroup["id"] for stepgroup in current_shift_stpgrps]
                    print(stepgroupids)

                    stepgroup_id_list.extend(stepgroupids)

                else:

                    print("No Stepgroup found for {} ".format(analyst))

        print("\n")

    return stepgroup_id_list

def parking_lot_tasks(dashboard_api_response):

    parked_tasks = []

    for ap in dashboard_api_response['parkingLot']['unscheduledTasks']:
        for task in ap['taskIds']:
            #print(task)
            parked_tasks.append(task)

    return parked_tasks

def create_schedule(base_url, token, req):
    """
    This function returns the list of tasks accessible by the authenticated user.

    :param base_url: The base URL of the application
    :param token: A bearer token obtained by authentication
    :return:
    """
    response = do_request('POST', base_url, '/api/schedule/create', req, token)
    return response

def mark_scheduled_task_completed(stepgroup_ids,token, duplicate_schedule_id):

    for stepgroup_id in stepgroup_ids:

        stepgroup_id = int(stepgroup_id)
        print(stepgroup_id)

        #fetching data for each step group and changing the step status to "Completed"
        resp = get_step_edit(base_url, token, stepgroup_id, duplicate_schedule_id)



        total_steps = len(resp['steps'])
        print("Total Steps ",len(resp['steps']))

        for step in range(total_steps):

            print("Step Status before changing : ",resp['steps'][step]['status'])
            resp['steps'][step]['status'] = 9
            print("Step Status after changing : ",resp['steps'][step]['status'])

            #print(resp['steps'][step])

        save_step_resp = save_step(base_url, token, resp, duplicate_schedule_id)
        print("-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#")
        print("-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#")

if __name__ == '__main__':

    #Reading property files
    with open("/home/appadmin/qc_scheduling/credentials.json",'r') as f:
        creds = json.load(f)

    print("******")
    print("Loading parameters/credentials")
    base_url=creds["base_url"]
    workspace_name=creds["workspace_name"]
    username=creds["username"]
    password=creds["password"]
    resource_map = {"Analysts":"0","Machines":'1'}
    print("******")


    token = get_bearer_token(base_url, workspace_name, username, password)

    tasks,Task_qc_df =  Task_qc_df(token)

    #fetching the last schedule
    schedule_id = get_schedule(base_url, token)
    print("Schedule Id : ",schedule_id)

    #creating a duplicate schedule
    duplicate_schedule_id = get_duplicate_schedule(base_url, token, int(schedule_id))
    print("Duplicate Schedule Id : ",duplicate_schedule_id)

    name,shiftCloseDate,shiftCloseShift,current_shift = generate_shift_name()
    print(name,shiftCloseDate,shiftCloseShift,current_shift)

    #this is the request needed to close the last shift
    back_end_schedule_data = {"scheduleId": str(duplicate_schedule_id),
                                "shiftCloseDate": shiftCloseDate,
                                "shiftCloseShift": shiftCloseShift}

    print("Backend schedule data : ",back_end_schedule_data)

    #calling the backend schedulinh api
    back_end_schedule_resp = back_end_schedule(base_url, token, back_end_schedule_data)
    print("Schedule created in the Backend")

    dashboard_api_response = get_schedule_dashboard(base_url, token, duplicate_schedule_id)

    stepgroup_ids_analysts = collect_stepgroups(dashboard_api_response,resource_map["Analysts"],current_shift)
    print("Analysts stepgroups : ",stepgroup_ids_analysts)

    stepgroup_ids_machines = collect_stepgroups(dashboard_api_response,resource_map["Machines"],current_shift)
    print("Machine stepgroups : ",stepgroup_ids_machines)

    stepgroup_ids = np.union1d(np.array(stepgroup_ids_analysts),np.array(stepgroup_ids_machines))
    print("Stepgroup IDs : ",stepgroup_ids)

    # resp = get_sch(base_url, token, duplicate_schedule_id)
    # resp["name"] = name
    #
    # update_schedule_name(base_url, token, resp)
    # print("Schedule name updated")
    #
    # publish_response = publish_schedule(base_url, token, duplicate_schedule_id)
    # print("Schedule Pubished for the shift")
    # print(publish_response)
    #

    #
    # if len(stepgroup_ids)>0:
    #
    #     mark_scheduled_task_completed(stepgroup_ids,token, duplicate_schedule_id)
    #
    #
    # #Parking lot Tasks
    # last_schedule_id = get_schedule(base_url, token)
    # print("Schedule Id : ",last_schedule_id)
    #
    # #Fetching the dashboard api resonse to get the parked tasks
    # dashboard_api_response = get_schedule_dashboard(base_url, token, last_schedule_id)
    #
    # #Fetching the parked tasks
    # parked_tasks = parking_lot_tasks(dashboard_api_response)
    #
    # print("Parking lot task obtained ")
    # print(parked_tasks)
    #
    # #Crate schedule only if Parked Tasks are found
    # if len(parked_tasks)>0:
    #
    #     req = create_schedule_request(parked_tasks)
    #     print("Schedule request Created")
    #
    #     resp = create_schedule(base_url, token, req)
    #     print("Schedule Created for next week")
    #
    #     print("Next Week Schedule ID : ",resp['id'])
    #
    # else:
    #
    #     print("There are no task in the Parking Lot")

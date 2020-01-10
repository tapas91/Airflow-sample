#Functions List:
#get_failed_tasks()
#raise_servnow_inc()

import requests
import logging
import json
from airflow.hooks.postgres_hook import PostgresHook

pghook=PostgresHook('AirflowDBname')
payload = {'sysparam_action':'insert','category':'software',
           'urgency':'2','assignment_group':'',
           'requested_by':''}
url = 'https://developer.service-now.com/api/now/v1/table/incident'
headers = {"Content-Type":"application/json","Accept":"application/json"}
#ServiceNow Credentials
user = ""
pwd = ""

def get_failed_tasks():
    """Fetch failed tasks from AirflowDB."""

    record=pghook.get_records(
        sql=f"""SELECT dag_id, execution_date FROM public.dag_run
            where state='failed'"""
    )
    return record


def raise_servnow_inc():
    """Function to create ServiceNow incident for each failed task."""

    failed_tasks = get_failed_tasks()
    if not failed_tasks:
        logging.info("No failed tasks fetched")
        return
    
    for dag_id, exec_date in failed_tasks:
        payload['short_descrition'] = dag_id
        payload['start_date'] = exec_date
        #REST-API call to raise an incident 
        response = requests.post(url=url, auth=(user, pwd),
                          headers=headers, data=json.dumps(payload))
        if response.status_code != 201:
            logging.error('Status: ', response.status_code)
            logging.error('Error Response:', response.json())
            return 'failed'
        content = response.json().get('result')
        #Logging the Incident Number
        logging.info('Incident {} raised'.format(content['number']))
    return 'success'

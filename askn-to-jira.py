import time
import datetime
import requests
from requests.auth import HTTPBasicAuth
import json
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
LOG_FILE = "./Logs/ceaws_asknow_to_jira_logging.log"
handler = RotatingFileHandler(LOG_FILE, maxBytes=2000000, backupCount=10)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

with open(r"./config.json") as jf:
   config_dict = json.load(jf)


service_now_user = config_dict.get('service_now').get('username')
service_now_pass = config_dict.get('service_now').get('password')

jira_user = config_dict.get('jira').get('username')
jira_pass = config_dict.get('jira').get('password')

ASK_NOW_BASE_URI = config_dict.get('service_now').get('asknow_url')
JIRA_BASE_URL = config_dict.get('jira').get('jira_base_url')

basic_auth_service_now = HTTPBasicAuth(service_now_user, service_now_pass)
basic_auth_jira = HTTPBasicAuth(jira_user, jira_pass)


proxies = config_dict.get('proxy')
header_json = {'Content-Type': "application/json"}

def get_next_week_date(days):
    today = datetime.date.today()
    next_date = today + datetime.timedelta(days=days)
    return str(next_date)


def get_requested_items():
    try:
        lst_of_inc_res = requests.get(f"{ASK_NOW_BASE_URI}/api/now/table/sc_req_item", 
                                              params={'sysparm_query': 'cat_item=dugfihifhfbiffljmfl;fm;knjbuguhljl^state=1'},
                                              proxies=proxies, auth=basic_auth_service_now, verify=False)
         return lst_of_inc_res
    except Exception as e:
       logger.error(f"Error Occured in get_requested_item- - {str(e)}")

def get_requested_item_variables(task_effective_number):
    try:
        res_variables = requests.get(f"{ASK_NOW_BASE_URI}/api/now/table/sc_item_option_mtom", 
                                              params={'request_item': task_effective_number},
                                              proxies=proxies, auth=basic_auth_service_now, verify=False)
        return res_variables
    except Exception as e:
          logger.error(f"Error Occuered - {str(e)}")


def get_val_and_field(link):
    try:
       value = requests.get(link, proxies=proxies, auth=basic_auth_service_now, verify=False)
       data = value.json.get('result')
       v = data.get('value')
       field = requests.get(data.get('item_option_new').get('link'), proxies=proxies, auth=basic_auth_service_now, verify=False)
       k = field.json().get('result').get('sys_name')
       return k, v
    except Exception as e:
          logger.error(f"Error Occuered - {str(e)}")

def update_service_now_task(jira_id, service_now_src_id):
    jira_url = "{}/brose/{}".format(JIRA_BASE_URL, jira_id)
    payload = {
            "state: "3",
            "close_notes": f"Jira created Ref: ({jira_url}), request would be tracked further under({jira_id}) - Resolved",
            "comments": f"jira Created Ref: ({jira_url}), request would be tracked further under({jira_id}) - Resolved"
            }

            try:
               res = requests.get(f"{ASK_NOW_BASE_URL/api/now/table/sc_task",
                                   params={'sysparm_query': 'request_item={}'.format(service_now_src_id)},
                                   proxies=proxies, auth=basic_auth_service_now, verify=False)
               if res.status_code == 200:
                  for i in res.json()['result']:
                      res1 = requests.patch(f"{ASK_NOW_BASE_URL/api/now/table/task/{i.get('sys_id')}",
                                   data=json.dumps(payload), headers=header_json,
                                   proxies=proxies, auth=basic_auth_service_now, verify=False)
                      logger.info("Info")
                      time.sleep(1)
               res2 = requests.patch(f"{ASK_NOW_BASE_URL/api/now/table/task/{i.get('sys_id')}",
                                   data=json.dumps(payload), headers=header_json,
                                   proxies=proxies, auth=basic_auth_service_now, verify=False)
               logger.info("Added some Info")
              except Exception as e:
                logger.error("error is :")


def search_jira_ticket(jql):

     payload = {

           'jql': jql
            }

            try:
                res = requests.post(f"{JIRA_BASE_URL}/rest/api/latest/search", data=json.dumps(payload),
                                     auth=basic_auth_jira, headers=header_json, verify=False)
                if res.status_code == 200:
                   return res.json().get("issues")
             exceprt Exception as e:
                logger.error("Exception as Error is:")

def create_jira_ticket(fields_mapping, inc_res):
    request_types = ['DDoS', 'Cloud Proxy - Squid Proxy', Network Firewall', 'Guardrails', 'SCP', 'Automation and Orchestration', 'Alerts', 'Event Monitoring']

    payload = {

       "fields": {
            "project":
              {
                "key": "CEAWS"              
              },
            "summary": "",
            "description": "",
            "issuetype": {
                     "name": ""
                        },
            "labels": [],
            "priority": {
                      "name": ""
                    },
            "duedate": "",
            "assignee": {
                "name": ""
                   },
            "customfield_17715": "",  # ATC Email
            "customfield_21004": "",  #Asset ID
            "customfield_17283": "",  # Origin Name
            "customfield_18281": ""   #Service Name
         }
      }

     if fields_mapping.get("Request Type") in request_types:
        payload["fields"]["issuetype"]["name"] = "IS Intake"
        payload["fields"]["labels"] = [fields_mapping.get("Request Type")]
        if fields_mapping.get("Request Type") == "Cloud Proxy - Squid Proxy":
           payload["fields"]["labels"] = ["CloudProxy_Squidproxy"]
        elif fields_mapping.get("Request Type") == "Netwrok Firewall"
           payload["fields"]["labels"] = ["Netwrok_Firewall"]
        elif fields_mapping.get("Request Type") == "Automation and Orchestration"
           payload["fields"]["labels"] = ["AutomationOrchestration"]

     elif fields_mapping.get("Request Type") in ["Exclusions", "Suppression"]:
          payload["fields"]["issuetype"]["name"] = "Task"
          payload["fields"]["labels"] = [fields_mapping.get("Request Type")]

     elif fields_mapping.get("Request Type") == "Issues or Queries":
          payload["fields"]["issuetype"]["name"] = "Queries"
          payload["fields"]["labels"] = ["Issues_or_Queries"]
     else:
          payload["fields"]["issuetype"]["name"] = "Other"
          payload["fields"]["labels"] = ["Others"]

     if fields_mapping.get("Priority").lower() == "high":
        payload["fields"]["priority"]["name"] = "High"
        payload["fields"]["dueDate"] = get_next_week_date(7)

     if fields_mapping.get("Priority").lower() == "medium":
        payload["fields"]["priority"]["name"] = "Medium"
        payload["fields"]["dueDate"] = get_next_week_date(14)

     if fields_mapping.get("Priority").lower() == "low":
        payload["fields"]["priority"]["name"] = "Low"
        payload["fields"]["dueDate"] = get_next_week_date(21)

     if fields_mapping.get("Priority").lower() == "trivial":
        payload["fields"]["priority"]["name"] = "Low"
        payload["fields"]["dueDate"] = get_next_week_date(21)

     payload["fields"]["summary"] = f"{fields_mapping.get('Request Type')} Request - {fields_mapping.get('Account Name')}" \
                                    f" : {fields_mapping.get('Account Number or Unique Identifier')} Request"

    payload["fields"]["description"] = f"Asknow ID - {fields_mapping.get('Request Type')}\n\n" \ 
                                       f"Asknow URL - {ASK_NOW_BASE_URI}/sc_req_item.do?sys_id={inc_res.get('sys_id')}\n\n" \
                                       f"Account Name - {fields_mapping.get('Account Number or Unique Identifier')}\n" \
                                       f"Cloud Account Owner - {fields_mapping.get('Email Address of Cloud Account Owner or Administrator')}\n" \
                                       f"Email Address - {fields_mapping.get('Email Address of Cloud Account Owner or Administrator')}\n" \
                                       f"Cloud Account Provider - {fields_mapping.get('Cloud Account Provider')}\n\n" \
                                       f"Business Justification - {fields_mapping.get('Business Justification and/or Additional Information')}"

    payload["fields"]["assignee"]["name"] = "nameoftheuser"
    payload["fields"]["customfield_17715"] = fields_mapping.get("Email Address of Cloud Account Owner or Administrator")
    payload["fields"]["customfield_21004"] = fields_mapping.get("Account Number or Unique Identifier")
    payload["fields"]["customfield_17283"] = fields_mapping.get("Account Name")
    payload["fields"]["customfield_18381"] = fields_mapping.get("Cloud Service Provider")

    jql = f'Project = CEAWS and summary - "{payload["fields"]["summary"]}" AND labels = {payload["fields"]["labels"][0]}'
    search_res = search_jira_ticket(jql)

    if search_res:
       return search_res[0], 200
    try:
      response = requests.get(f"{JIRA_BASE_URL}/rest/api/latest/issue", data=json.dumps(payload), headers=header_json, auth=basic_auth_jira, verify=False)
          return response.json(), response.status_code
    except Exception as e:
      logger.error("Error is : ")


def service_now():
    for inc_res in get_requested_items().json()['result']:
        fields_mapping = {
          "Owner Info set by widget": "",
          "Cloud Service Provider": "",
          "Special Instructions": "",
          "Request Type": "",
          "Request Source": "",
          "Request Details": "",
          "Account Number or Unique Identifier": "",
          "Externam+reference_id": "",
          "Business Justification and/or Additional Information": ""
          "Email Address of cloud Account Owner or Administrator": "",
          "Priority": "",
          "Requested for": "",
          "var_name_arr": "",
          "Account Name": ""
       }

       variables = get_requested_item_variables(inc_res['number'])
       for variable in variables.json()['result']:
        k, v = get_val_and_field(variable['sc_item_option']['link'])
        if k in fields_mapping:
            fields_mapping['Request Type'] in ['Exclusions', 'Suppression'] and inc_res['approval'] != 'approved':
            pass
        else:
           jira_ticket_obj, status_code = create_jira_ticket(fields_mapping, inc_res)
           if status_code == 201 or status_code == 200:
               update_service_now_task(jira_ticket_obj['key'], inc_res.get('sys_id'))


if __name__ == '__main__':
    service_now()

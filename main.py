import base64
import json
import config
from google.cloud import bigquery
import requests

def transform_data(ligne,data):
    contact = {
            "email": data.at[ligne,"email"],
            "name": data.at[ligne,"first_name"] + " " + data.at[ligne,"last_name"],
            "properties": [
                    {"property": "firstname", "value": data.at[ligne,"first_name"]},
                    {"property": "lastname", "value": data.at[ligne,"last_name"]},
                    {"property": "company", "value": ""},
                    {"property": "associatedcompanyid", "value": ""}
                    ]
            }
    return contact

def create_contacts_to_hs():
    #global blob
    global contacts

    sql = "SELECT * FROM covid_data.malades"

    bq_client = bigquery.Client(project=config.config_vars['project_id'])
    rows_df = bq_client.query(sql).to_dataframe()
    rows_df_length = len(rows_df.index)

    print('Create Contacts to HubSpot.')

    for x in range(rows_df_length):
        try:
            contact = transform_data(x, rows_df)
            email = contact['email']
            url = f"https://api.hubapi.com/contacts/v1/contact/createOrUpdate/email/{email}/?hapikey=d8d8f703-376c-4ab3-998f-2205a8df43be"

            r = requests.post(url, json= contact)

            name = contact["name"] 
            status_code = r.status_code
            print(str(status_code) + ' - Contact: '+ name)

            if status_code != 200:
                logger.info(r.json())

            contact['vid'] = r.json().get('vid')

        except Exception as e:
            print(e.args)
            pass

def get_data(event, context):
    message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = json.loads(message)
    if pubsub_message["protoPayload"]["serviceData"]["jobCompletedEvent"]["job"]["jobConfiguration"]["query"]["statementType"]=="UPDATE":
        print("Update")
        create_contacts_to_hs()

"""
This code is written by Yashika Sharma
on Date 2-07-2024
version1.0.0

Gerrit All Revisions
"""


# Importing required libraries
import re
import json
import pandas as pd
import requests as req
from MessageSender import MessageSender, MessageType

# Klera Output Details
klera_meta_out = {
    "All Revisions": {
        "DSTID": "Gerrit_AllRevisionsV1.0.0",
        "Primary Key": {
            "isrowid": True,
            "isvisible": True,
            "datatype": "STRING"
        }
    },
    "Error Details": {
        "DSTID": "Error_Details_V1.0.0",
        "Primary Key": {
            "isrowid": True,
            "isvisible": True,
            "datatype": "STRING"
        }
    }
}

# Klera Input Details
klera_in_details = {
    "Instance_Url": {
        "description": "Instance Url",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": False,
        "required": True
    },
    "Username": {
        "description": "Username",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": False,
        "required": True
    },
    "Password": {
        "description": "Password",
        "datatype": ["STRING"],
        "argtype": "Param",
        "masked": True,
        "required": True
    },
    "Inputs": {
        "description": "Enter the Change Id/Number",
        "datatype": ["STRING"],
        "argtype": "Data",
        "required": True,
        "multiplerows": True
    }
}

# Defining main data dictionary and error related data dictionary
kleradict = {}
klera_error_dict = {}

instance_url = Instance_Url
username = Username
password = Password

# Defining the other variables used in the code
n=100
s=0
change_id = ""
revision_id = ""
abort = False
defaultvalue = None
http_status_code = None
http_error_message = None
columns_list = ["Id","Triplet Id","Project","Branch","Topic","Change ID","Subject","Status","Owner","Created","Updated","Insertions","Deletions","Total Comment Count","Unresolved Comment Count","Has Review Started","Meta Rev Id","Number","Virtual Id Number","Current Revision Number","Current Revision","Revision Id","Kind","Uploader","Ref","Ref (HTTP)","Ref (SSH)","Ref Branch"]

#Defining Request Timeouts
timeout = 600

for input in Inputs:

    try:

        #Abort Handling
        if abort or klera_message_sender.is_operation_aborted():
            abort = True
            break

        url = instance_url + "/changes/?q=change:" + input + "&o=ALL_REVISIONS&n=" + n + "s=" + s
        auth = (username, password)

        try:
            response = re.get(url,auth=auth, headers=headers, timeout = timeout)
        except re.exceptions.Timeout:
            raise Exception("Request Timeout")

        if response.status_code != 200:
            http_status_code = response.status_code
            http_error_message = response.text
            raise Exception

        response = req.get(url, auth=auth)
        response = response.text[4:]

        data = json.loads(response)

        for item in data:

            # Abort Handling
            if abort or klera_message_sender.is_operation_aborted():
                abort = True
                break

            kleradict.setdefault("Id", []).append(item['id'])
            kleradict.setdefault("Triplet Id", []).append(item['triplet_id'])
            kleradict.setdefault("Project", []).append(item['project'])
            kleradict.setdefault("Branch", []).append(item['branch'])
            kleradict.setdefault("Topic", []).append(item['topic'])
            kleradict.setdefault("Change ID", []).append(item['change_id'])
            change_id = item['change_id']
            kleradict.setdefault("Subject", []).append(item['subject'])
            kleradict.setdefault("Status", []).append(item['status'])
            kleradict.setdefault("Owner", []).append(item['owner']['_account_id'])
            kleradict.setdefault("Created", []).append(item['created'])
            kleradict.setdefault("Updated", []).append(item['updated'])
            kleradict.setdefault("Insertions", []).append(item['insertions'])
            kleradict.setdefault("Deletions", []).append(item['deletions'])
            kleradict.setdefault("Total Comment Count", []).append(item['total_comment_count'])
            kleradict.setdefault("Unresolved Comment Count", []).append(item['unresolved_comment_count'])
            kleradict.setdefault("Has Review Started", []).append(item['has_review_started'])
            kleradict.setdefault("Meta Rev Id", []).append(item['meta_rev_id'])
            kleradict.setdefault("Number", []).append(item['_number'])
            kleradict.setdefault("Virtual Id Number", []).append(item['virtual_id_number'])
            kleradict.setdefault("Current Revision Number", []).append(item['current_revision_number'])
            kleradict.setdefault("Current Revision", []).append(item['current_revision'])

            for rev_id, revision in item['revisions'].items():

                # Abort Handling
                if klera_message_sender.is_operation_aborted():
                    abort = True
                    break

                kleradict.setdefault("Revision Id", []).append(rev_id)
                revision_id = rev_id
                kleradict.setdefault("Kind", []).append(revision['kind'])
                kleradict.setdefault("Number", []).append(revision['_number'])
                kleradict.setdefault("Uploader", []).append(revision['uploader']['_account_id'])
                kleradict.setdefault("Created", []).append(revision['created'])
                kleradict.setdefault("Ref", []).append(revision['ref'])
                kleradict.setdefault("Ref (HTTP)", []).append(revision['fetch']['http']['url'])
                kleradict.setdefault("Ref (SSH)", []).append(revision['fetch']['ssh']['url'])
                kleradict.setdefault("Ref Branch", []).append(revision['branch'])

                kleradict.setdefault("Primary Key", []).append(change_id + "-" + revision_id)

        for val in columns_list:
            if val in kleradict:
                pass
            else:
                kleradict.setdefault(val, []).append(defaultvalue)

        s = n + s

        out_df1 = pd.DataFrame(data=kleradict)
        out_dict1 = {'All Revisions': out_df1}
        klera_dst = [out_dict1]

        # Create a data block
        data_block = {}
        data_block['klera_dst'] = klera_dst
        data_block['klera_meta_out'] = klera_meta_out
        klera_message_block = {}

        # Message Type
        klera_message_block['message_type'] = MessageType.DATA
        klera_message_block['data_block'] = data_block
        klera_message_sender.push_data_to_queue(klera_message_block)

        kleradict.clear()

    except Exception as e:
        klera_error_dict.setdefault('Http Status Code', []).append(str(http_status_code))
        klera_error_dict.setdefault('Http Error Message', []).append(http_error_message)
        klera_error_dict.setdefault("Response", []).append(str(e))
        klera_error_dict.setdefault("Input", []).append(str(input))

        out_df2 = pd.DataFrame(data=klera_error_dict)
        out_dict2 = {'Error Details': out_df2}
        klera_dst = [out_dict2]

        data_block = {}
        data_block['klera_dst'] = klera_dst
        data_block['klera_meta_out'] = klera_meta_out
        klera_message_block = {}

        # Message Type
        klera_message_block['message_type'] = MessageType.DATA
        klera_message_block['data_block'] = data_block
        klera_message_sender.push_data_to_queue(klera_message_block)
        klera_error_dict.clear()

        if e == "Request Timeout":
            abort = True
            break

if(kleradict):
    out_df1 = pd.DataFrame(data=kleradict)
    out_dict1 = {'All Revisions': out_df1}
    klera_dst = [out_dict1]

    # Create a data block
    data_block = {}
    data_block['klera_dst'] = klera_dst
    data_block['klera_meta_out'] = klera_meta_out
    klera_message_block = {}

    # Message Type
    klera_message_block['message_type'] = MessageType.DATA
    klera_message_block['data_block'] = data_block
    klera_message_sender.push_data_to_queue(klera_message_block)

    kleradict.clear()


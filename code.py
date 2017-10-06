
import base64
import boto3
import datetime
import json

from monetate.analytics_db import db_engine

# The analytics database is AWS Aurora, which implements the same interface as MySQL.
#
# Assume there exists a table called 'account_session_count' which has columns:
#   account_id INTEGER PRIMARY_KEY,
#   session_count INTEGER,
#   last_session_timestamp INTEGER
#
# You can execute queries like this:
#   db_engine.execute("SELECT * FROM account_session_count")


S3_BUCKET_NAME = 'monetate-session-stream'
S3_CLIENT = boto3.client('s3')


def my_function(event, context):
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)

        # Make timestamp human readable.  isoformat() returns a string like 'YYYY-MM-DDTHH:MM:SS.uuuuuuu'
        data['timestamp'] = datetime.datetime.utcfromtimestamp(data['timestamp']).isoformat()

        # If sessionizer was run in debug mode, the debug key appears.  Don't show it to end-users.
        del data['debug']

        # Include 'firstProductViewed'
        data['firstProductViewed'] = data['productsViewed'][0]['productId']

        # Product Management wanted to rename the key 'urls' to 'pagesViewed'
        data['pagesViewed'] = data['urls']

        # Append record to s3
        s3_key = str(data['account_id']) + '/session-stream/' + data['timestamp'][:10].replace('-', '/')
        s3_obj = S3_CLIENT.get_object(S3_BUCKET_NAME, s3_key)
        file_contents = s3_obj['Body'].read()  # Download the s3 object and return contents as a string
        file_contents += json.dumps(data) + '\n'
        s3_obj.put(Body=file_contents)  # Upload the string back up to s3

        # Increment session count
        query_results = db_engine.execute(
            "SELECT account_session_count FROM account_session_count WHERE account_id = " + str(data['accountId'])
        )
        session_count = query_results[0][0]
        db_engine.execute("UPDATE account_session_count SET session_count = " + str(session_count + 1) +
                          ", last_timestamp = " + data['timestamp'] + " WHERE account_id = " + data['accountId'])


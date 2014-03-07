import json
import os
import time

# Sample code authorization support.
import auth

# Set this to your sample project id.
PROJECT_ID = 317752944021
JOB_ID = 'ch06_%d' % int(time.time())
body = {
    'jobReference': {
        'jobId': JOB_ID
        },
    'configuration': {
        'load': {
            'destinationTable': {
                'projectId': PROJECT_ID,
                'datasetId': 'ch06',
                # You can update this for each example.
                'tableId': 'example1'
                }
            }
        }
    }
loadConfig = body['configuration']['load']
# Setup the job here.
# load[property] = value
loadConfig['schema'] = {
    'fields': [
        {'name':'string_f', 'type':'STRING'},
        {'name':'boolean_f', 'type':'BOOLEAN'},
        {'name':'integer_f', 'type':'INTEGER'},
        {'name':'float_f', 'type':'FLOAT'},
        {'name':'timestamp_f', 'type':'TIMESTAMP'}
        ]
    }

from apiclient.http import MediaFileUpload
upload = MediaFileUpload('sample.csv',
                         mimetype='application/octet-stream',
                         # This enables resumable uploads.
                         resumable=True)
# End of job configuration.

bq = auth.build_bq_client()
jobs = bq.jobs()

start = time.time()
# Create the job.
result = jobs.insert(projectId=PROJECT_ID,
                     body=body,
                     # Include the payload in the creation request.
                     media_body=upload).execute()
print json.dumps(result, indent=2)
# Wait for completion.
done = False
while not done:
    time.sleep(5)
    result = jobs.get(projectId=PROJECT_ID, jobId=JOB_ID).execute()
    print "%s %ds" % (result['status']['state'], time.time() - start)
    done = result['status']['state'] == 'DONE'
# Print all errors and warnings.
for err in result['status'].get('errors', []):
    print json.dumps(err, indent=2)
# Check for failure.
if 'errorResult' in result['status']:
    print 'FAILED'
    print json.dumps(result['status']['errorResult'], indent=2)
else:
    print 'SUCCESS'

#!/usr/bin/bash
# Chapter 5 bash commands and responses.
# Header?
# Copyright?
# License?

### Expect AUTH_TOKEN to be set to a current OAuth2 Auth Token. 
### If you don't have an auth token, run auth.py to get one.

### Setting up some handy shortcuts.
BASE_URL=https://www.googleapis.com/bigquery/v2
PROJECT_URL=${BASE_URL}/projects/bigquery-e2e
DATASETS_URL=${BASE_URL}/projects/bigquery-e2e/datasets
DATASET_URL=${BASE_URL}/projects/bigquery-e2e/datasets/scratch
TABLES_URL=${BASE_URL}/projects/bigquery-e2e/datasets/temp/tables
TABLEDATA_URL=${DATASET_URL}/tables/shakespeare_quantiles/data
JOBS_URL=${BASE_URL}/projects/bigquery-e2e/jobs

### Reading the bigquery-e2e:application_logs dataset
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    "${PROJECT_URL}/datasets/application_logs"
## Output:
# {
# "kind": "bigquery#dataset",
# "etag": "\"4PTsVxg68bQkQs1RJ1Ndewqkgg4/KOb9IHTeiiCy_ICxng0jrzYn6Zk\"",
# "id": "bigquery-e2e:application_logs",
# "selfLink": "https://www.googleapis.com/bigquery/v2/projects/bigquery-
#e2e/datasets/application_logs",
# "datasetReference": {
#  "datasetId": "application_logs",
#  "projectId": "bigquery-e2e"
# },
#...
#}

### Using startIndex
bq query \
    --destination_table=scratch.shakespeare_quantiles \
     --max_rows=0 \
     "select quantiles(word_count) from publicdata:samples.shakespeare"
## Output:
# Waiting on bqjob_r8717055dd1ebad8_0000014070d5e4b8_1 ... (0s) Current
# status: DONE

curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    "${TABLEDATA_URL}?maxResults=1&startIndex=99"
## Output:
# {
#  "kind": "bigquery#tableDataList",
#  "etag": "\"yBc8hy8wJ370nDaoIj0ElxNcWUg/vk5JRNdt-25J-ICZ34R0Dqpt1Fc\"",
#  "totalRows": "100",
#  "rows": [
#   {
#    "f": [
#     {
#      "v": "995"
#     }
#    ]
#   }
#  ]
# }

### Using a pageToken
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    "${TABLEDATA_URL}?maxResults=1"
## Output:
#  {
#  "kind": "bigquery#tableDataList",
#  "etag": "\"yBc8hy8wJ370nDaoIj0ElxNcWUg/hDRfxHX8yY1GRPpTthhjzhvMMj8\"",
#  "totalRows": "100",
#  "pageToken": "1@1376284335714",
#  "rows": [
#   {
#    "f": [
#     {
#      "v": "1"
#     }
#    ]
#   }
#  ]
# }

curl  -H "Authorization: Bearer ${AUTH_TOKEN}”
    "${TABLEDATA_URL}?maxResults=1&pageToken=1@1376284335714"
## Output:
# {
#  "kind": "bigquery#tableDataList",
#  "etag": "\"yBc8hy8wJ370nDaoIj0ElxNcWUg/wkrQkIirE3G6Vvv0_ZnfZOk_V64\"",
#  "totalRows": "100",
#  "pageToken": "2@1376284335714",
#  "rows": [
#   {
#    "f": [
#     {
#      "v": "1"
#     }
#    ]
#   }
#  ]
# }

### Using Projections
curl -H "Authorization: Bearer ${AUTH_TOKEN}"   
    "${JOBS_URL}?maxResults=1&projection=minimal"
## Output:
# {
#  "kind": "bigquery#jobList",
#  "etag": "\"yBc8hy8wJ370nDaoIj0ElxNcWUg/UakzbU_RhC8kGP0ve9SOqAWE6Ls\"",
# "nextPageToken": "1374677424654-...",
#  "jobs": [
#   {
#   "id": "bigquery-e2e:bqjob_r1ef2a0ae815fa433_000001401128cb0b_1",
#    "kind": "bigquery#job",
#    "jobReference": {
#     "projectId": "bigquery-e2e",
#     "jobId": "bqjob_r1ef2a0ae815fa433_000001401128cb0b_1"
#    },
#    "state": "DONE",
#    "statistics": {
#     "startTime": "1374677431634",
#     "endTime": "1374677458425",
#     "load": {
#      "inputFiles": "1",
#      "inputFileBytes": "3",
#      "outputRows": "1",
#      "outputBytes": "0"
#     }
#    },
#    "status": {
#     "state": "DONE"
#    }
#   }
#  ]
# }

curl -H "Authorization: Bearer ${AUTH_TOKEN}" \
    "${JOBS_URL)?maxResults=1&projection=full"
## Output:
# {
#  "kind": "bigquery#jobList",
# "etag": "\"yBc8hy8wJ370nDaoIj0ElxNcWUg/ytdMysUYGZY_OZKW01VMUuMdT0k\"",
#  "nextPageToken": "1374677424654-...",
#  "jobs": [
#   {
#    "id": "bigquery-e2e:bqjob_r1ef2a0ae815fa433_000001401128cb0b_1",
#    "kind": "bigquery#job",
#    "jobReference": {
#     "projectId": "bigquery-e2e",
#     "jobId": "bqjob_r1ef2a0ae815fa433_000001401128cb0b_1"
#   },
#    "state": "DONE",
#    "statistics": {
#     "startTime": "1374677431634",
#     "endTime": "1374677458425",
#     "load": {
#      "inputFiles": "1",
#      "inputFileBytes": "3",
#      "outputRows": "1",
#      "outputBytes": "0"
#     }
#    },
#    "configuration": {
#     "load": {
#      "schema": {
#       "fields": [
#        {
#         "name": "f1",
#         "type": "STRING"
#        },
#        {
#         "name": "f2",
#         "type": "INTEGER"
#        },
#        {
#         "name": "f3",
#         "type": "FLOAT"
#        }
#       ]
#      },
#      "destinationTable": {
#       "projectId": "bigquery-e2e",
#       "datasetId": "scratch",
#       "tableId": "foo"
#      },
#      "writeDisposition": "WRITE_TRUNCATE",
#      "maxBadRecords": 0
#     }
#    },
#    "status": {
#     "state": "DONE"
#    }
#   }
#  ]
# }

### Applying field restrictions
curl \
    -H "Authorization: Bearer ${AUTH_TOKEN}" \
   "${BASE_URL}/projects?alt=json&fields=projects(id),totalItems"
## Output:
# {
#  "projects": [
#   {
#    "id": "bigquery-e2e"
#   },
#   {
#    "id": "420824040427"
#   }
#  ],
#  "totalItems": 3
# }

### Using ETags and the If-None-Match header
FOO_URL=${BASE_URL}/projects/bigquery-e2e/datasets/scratch/tables/foo
curl -H "Authorization: Bearer ${AUTH_TOKEN}" \ 
    "${FOO_URL}?fields=etag,lastModifiedTime"
## Output:
# {
#  "etag": "\"yBc8hy8wJ370nDaoIj0ElxNcWUg/gS3ul2baST3PwOoDSGXgugy2uws\"",
#  "lastModifiedTime": "1374677458335"
# }
## Copy the e-tag from the response:
ETAG=\"yBc8hy8wJ370nDaoIj0ElxNcWUg/gS3ul2baST3PwOoDSGXgugy2uws\"
curl   -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "If-None-Match: ${ETAG}" \
   "${URL}?fields=etag,lastModifiedTime"
## Output: <empty>

bq load --replace scratch.foo foo.csv "f1:string,f2:integer,f3:float"
## Output:
# Waiting on bqjob_r1acbcee37ed9abeb_000001407034129d_1 ... (26s) 
# Current status: DONE
curl   -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "If-None-Match: ${ETAG}" \
   "${URL}?fields=etag,lastModifiedTime"
## Output:
# {
#  "etag": "\"yBc8hy8wJ370nDaoIj0ElxNcWUg/rlB4v5eu0LBEfFBRBW7-oRFArSQ\"",
#  "lastModifiedTime": "1376270939965"
# }

#### BigQuery Collections

### Projects.list()
curl -H "Authorization: Bearer ${AUTH_TOKEN}" \
  https://www.googleapis.com/bigquery/v2/projects?alt=json
## Output:
# {
# "projects": [
#   {
#    "kind": "bigquery#project",
#    "id": "540617388650",
#    "numericId": "540617388650",
#    "projectReference": {
#    "projectId": "540617388650"
#    },
#    "friendlyName": "API Project"
#   },
#   {
#    "kind": "bigquery#project",
#    "id": "bigquery-e2e",
#    "numericId": "857243983440",
#    "projectReference": {
#     "projectId": "bigquery-e2e"
#    },
#    "friendlyName": "Bigquery End-to-End"
#   },
# "totalItems": 2
# }

### Datasets.insert()
DATASET_REF="{'datasetId': 'temp', 'projectId': 'bigquery-e2e'}"
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST \
    -d "{'datasetReference': ${DATASET_REF}}" \
    "${DATASETS_URL}"
## Output:
# {
# ...
# "datasetReference": {
#   "datasetId": "temp",
#   "projectId": "bigquery-e2e"
#  },
#  "access": [
#   {
#    "role": "READER",
#    "specialGroup": "projectReaders"
#   },
#   {
#    "role": "WRITER",
#    "specialGroup": "projectWriters"
#   },
#   {
#    "role": "OWNER",
#    "specialGroup": "projectOwners"
#   }
#  ],
#  "creationTime": "1376367421192",
#  "lastModifiedTime": "1376367421192"
# }

### Datasets.get()
LOGS_URL=${DATASETS_URL}/application_logs
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    ${LOGS_URL}?fields=creationTime,datasetReference(datasetId)"
## Output:
# {
#  "datasetReference": {
#   "datasetId": "application_logs"
#  },
#  "creationTime": "1374439672882"
# }

### Datasets.list()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}"  "${DATASETS_URL}?maxResults=1"
## Output:
# {
#  "nextPageToken": "application_logs",
#  "datasets": [
#   {
#    "kind": "bigquery#dataset",
#    "id": "bigquery-e2e:application_logs",
#    "datasetReference": {
#     "datasetId": "application_logs",
#     "projectId": "bigquery-e2e"
#    }
#   }
#  ]
# }

### Datasets.update()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X PUT \
    -d "{'datasetReference': ${DATASET_REF}, \
         'friendlyName': 'my dataset'}" \
    "${BASE_URL}/projects/bigquery-e2e/datasets/temp"
## Output:
# {
#  "datasetReference": {
#   "datasetId": "temp",
#   "projectId": "bigquery-e2e"
#  },
#  "friendlyName": "my dataset",
#  "access": [
#   {
#    "role": "READER",
#    "specialGroup": "projectReaders"
#   },
#   {
#    "role": "WRITER",
#    "specialGroup": "projectWriters"
#   },
#   {
#    "role": "OWNER",
#    "specialGroup": "projectOwners"
#   }
#  ],
#  "creationTime": "1376367421192",
#  "lastModifiedTime": "1376369547951"
# }

### Datset.patch()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json"    \
    -X PATCH     
    -d "{'friendlyName': 'Bob\'s dataset'}" \
    "${BASE_URL}/projects/bigquery-e2e/datasets/temp"
## Output:
# {
# ...
#  "datasetReference": {
#   "datasetId": "temp",
#   "projectId": "bigquery-e2e"
#  },
#  "friendlyName": "Bob's dataset",
#  "access": [ ... ],
#  "creationTime": "1376367421192",
#  "lastModifiedTime": "1376370888255"
# }

### Datasets.delete()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -X DELETE   \
    "${BASE_URL}/projects/bigquery-e2e/datasets/temp"
## Output: <none>

### Tables.insert()
SCHEMA="{'fields': [{'name':'foo', 'type': 'STRING'}]}"
TABLE_REF="{'tableId': 'table1', \
    'datasetId': 'temp', \
    'projectId': 'bigquery-e2e'}"
curl -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST \
    -d "{'tableReference': ${TABLE_REF}, \
         'schema': ${SCHEMA}}" \
    "${TABLES_URL}"
## Output:
# {
# ...
#  "tableReference": {
#   "projectId": "bigquery-e2e",
#   "datasetId": "temp",
#   "tableId": "table1"
#  },
# "schema": {
#   "fields": [
#    {
#     "name": "foo",
#     "type": "STRING"
#    }
#   ]
#  },
#  "creationTime": "1376533497018",
#  "lastModifiedTime": "1376533497018"
# }

### Tables.get()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" 
    -X GET \
    "${TABLES_URL}/table1"
## Output:
# {
#  ...
#  "tableReference": {
#   "projectId": "bigquery-e2e",
#   "datasetId": "temp",
#   "tableId": "table1"
#  },
#  "schema": {
#   "fields": [
#    {
#     "name": "f1",
#     "type": "STRING"
#    }
#   ]
#  },
#  "numRows": "1",
#  "numBytes": "8",
#  "creationTime": "1376533497018",
#  "lastModifiedTime": "1376534761629"
# }

### Tables.list()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X GET \
    ${TABLES_URL}?maxResults=1"
## Output:
# {
#  ...
#  "nextPageToken": "table1",
#  "tables": [
#  {
#    "kind": "bigquery#table",
#    "id": "bigquery-e2e:temp.table1",
#    "tableReference": {
#    "projectId": "bigquery-e2e",
#    "datasetId": "temp",
#    "tableId": "table1"
#    }
#   }
#  ],
#  "totalItems": 2
# }

### Tables.update()
SCHEMA2="{'fields': [ \
    {'name':'foo', 'type': 'STRING'}, \
    {'name': 'bar', 'type': 'FLOAT'}]}"
TABLE_JSON="{'tableReference': ${TABLE_REF}, 'schema': ${SCHEMA2}}"
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X PUT \
    -d "${TABLE_JSON}" \
    "${TABLES_URL}/table1"
## Outout:
# {
# ...
# "tableReference": {
#   "projectId": "bigquery-e2e",
#   "datasetId": "temp",
#   "tableId": "table1"
#  },
#  "schema": {
#   "fields": [
#    {
#     "name": "foo",
#     "type": "STRING"
#    },
#    {
#     "name": "bar",
#     "type": "FLOAT"
#    }
#   ]
#  },
#  "numBytes": "0",
#  "numRows": "0",
#  "creationTime": "1376537757773",
#  "lastModifiedTime": "1376537882648"
# }

### Tables.patch()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X PATCH -d "{'expirationTime': '1376539999999'}" \
    "${TABLES_URL}/table1"
## Output:
# {
# ...
# "tableReference": {
#   "projectId": "bigquery-e2e",
#   "datasetId": "temp",
#   "tableId": "table1"
#  },
#  "schema": {
#   "fields": [
#    {
#     "name": "foo",
#     "type": "STRING"
#    },
#    {
#     "name": "bar",
#     "type": "FLOAT"
#    }
#   ]
#  },
#  "numBytes": "0",
#  "numRows": "0",
#  "creationTime": "1376537757773",
#  "expirationTime": "1376539999999",
#  "lastModifiedTime": "1376538270453"
# }

### Tables.delete()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -X DELETE \
    "${TABLES_URL}/table1"
## Output: <None>

### TableData.list()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X GET \
    "${TABLES_URL}/nested/data?pp=false"
## Output:
# {"kind":"bigquery#tableDataList", ... "totalRows":"3", 
# "rows":[
#   {"f":[{"v":"1"}, {"v":{"f":[{"v":"2.0"}, {"v":[{"v":"foo"}]}]}}]}, 
#   {"f":[{"v":"2"},{"v":{"f":[{"v":"4.0"}, {"v":[{"v":"bar"}]}]}}]}, 
#   {"f":[{"v":"3"},{"v":{"f":[{"v":"8.0"}, 
#     {"v":[{"v":"baz"},{"v":"qux"}]}]}}]}]}

### Jobs.insert()
JOB_REFERENCE="{'jobId': 'myHappyJob', 'projectId': 'bigquery-e2e'}"
JOB_CONFIG="{'query': {'query': 'select 17'}}"
JOB="{'jobReference': ${JOB_REFERENCE}, 'configuration': ${JOB_CONFIG}}"
curl  -H "Authorization: Bearer ${AUTH_TOKEN}"  \
    -H "Content-Type: application/json" \
    -X POST \
    -d "${JOB}" \
    "${JOBS_URL}"
## Output:
# {
# ...
#  "jobReference": {
#   "projectId": "bigquery-e2e",
#   "jobId": "myHappyJob"
#  },
#  "configuration": {
#   "query": {
#    "query": "select 17",
#    "destinationTable": {
#     "projectId": "bigquery-e2e",
#     "datasetId": "_0e32b38e1117b2fcea992287c138bd53acfff7cc",
#     "tableId": "anon5c03da1f543a2486eca295f285b40eb87b01ea84"
#    },
#    "createDisposition": "CREATE_IF_NEEDED",
#    "writeDisposition": "WRITE_TRUNCATE"
#   }
#  },
#  "status": {
#   "state": "RUNNING"
#  },
#  "statistics": {
#   "startTime": "1376685153396"
#  }

### Jobs.get()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    "${JOBS_URL}/myHappyJob"
## Output:
# {
# ...
# "jobReference": {
#   "projectId": "bigquery-e2e",
#   "jobId": "myHappyJob"
#  },
#  "configuration": {
#   "query": {
#    "query": "select 17",
#    "destinationTable": {
#     "projectId": "bigquery-e2e",
#     "datasetId": "_0e32b38e1117b2fcea992287c138bd53acfff7cc",
#     "tableId": "anon5c03da1f543a2486eca295f285b40eb87b01ea84"
#    },
#    "createDisposition": "CREATE_IF_NEEDED",
#    "writeDisposition": "WRITE_TRUNCATE"
#   }
#  },
#  "status": {
#   "state": "DONE"
#  },
#  "statistics": {
#   "startTime": "1376685153396",
#   "endTime": "1376685153696",
#   "query": {
#    "totalBytesProcessed": "0",
#   }
#  }
# }

### Jobs.list()
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X GET \
    "${JOBS_URL}?stateFilter=RUNNING&fields=jobs(jobReference(jobId))"
## Output:
# {
#  "jobs": [
#   {
#    "jobReference": {
#     "jobId": "bqjob_r29016a1bfe5187c8_000001408ce3ffc5_1"
#    }
#   },
#   {
#    "jobReference": {
#     "jobId": "bqjob_r239fa6e7bb78440_000001408ce3e8f2_1"
#    }
#   }
#  ]
# }

### Jobs.query()
QUERIES_URL=${BASE_URL}/projects/bigquery-e2e/queries
QUERY="{'query': 'select 17', 'timeoutMs': 1000000}"
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST  \
    -d "${QUERY}" \
    "${QUERIES_URL}"
## Output:
# {
#  "kind": "bigquery#queryResponse",
#  "schema": {
#   "fields": [
#    {
#     "name": "f0_",
#     "type": "INTEGER",
#     "mode": "NULLABLE"
#    }
#   ]
#  },
#  "jobReference": {
#   "projectId": "bigquery-e2e",
#   "jobId": "job_af9f9b1c2c6d4fa9bb7b75c9c84b7d5e"
#  },
#  "totalRows": "1",
#  "rows": [
#   {
#    "f": [
#     {
#      "v": "17"
#     }
#    ]
#   }
#  ],
#  "totalBytesProcessed": "0",
#  "jobComplete": true,
#  "cacheHit": true
# }


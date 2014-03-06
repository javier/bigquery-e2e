#!/usr/bin/bash
# Chapter 7 bash commands and responses.
# Header?
# Copyright?
# License?

### Expect AUTH_TOKEN to be set to a current OAuth2 Auth Token. 
### If you don't have an auth token, run auth.py to get one.

### Setting up some handy shortcuts.
QUERIES_URL= https://www.googleapis.com/bigquery/v2/projects/bigquery-e2e/queries
JOBS_URL=https://www.googleapis.com/bigquery/v2/projects/bigquery-e2e/jobs
DATASETS_URL=https://www.googleapis.com/bigquery/v2/projects/bigquery-e2e/datasets

### Running a query using the jobs.query() API.
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST \
    -d "{query:'select 17'}" \
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
#   "jobId": "job_7957d818b0a448199ac4b7f539d7d4c2"
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

### Starting a query using the jobs.insert() API
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  -H "Content-Type: application/json"     \
  -X POST  \
  -d "{configuration:{query: {query:'select 17'}}}" \
  "${JOBS_URL}"
### Output:
# {
#  "kind": "bigquery#job",
# … 
#  "jobReference": {
#   "projectId": "bigquery-e2e",
#   "jobId": "job_28c1344c67574b14818edd576049bc6b"
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
#   "startTime": "1378069553719"
#  }
# }

### Waiting for the query job to compoete.
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  -H "Content-Type: application/json" \
  -X GET \
  "${JOBS_URL}/job_28c1344c67574b14818edd576049bc6b"
## Output:
# {
#  "kind": "bigquery#job",
# …
# "jobReference": {
#   "projectId": "bigquery-e2e",
#   "jobId": "job_28c1344c67574b14818edd576049bc6b"
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
#   "startTime": "1378069553719",
#   "endTime": "1378069553874",
#   "totalBytesProcessed": "0",
#   "query": {
#    "totalBytesProcessed": "0",
#    "cacheHit": true
#   }
#  }
# }

### Shortcuts to dataset and table ids from the job.
DATASET_ID="_0e32b38e1117b2fcea992287c138bd53acfff7cc"
TABLE_ID="anon5c03da1f543a2486eca295f285b40eb87b01ea84"
DATASET_URL=${DATAETS_URL}/${DATASET_ID}
TABLE_URL=${DATASET_URL}/tables/${TABLE_ID}
TABLEDATA_URL=${TABLE_URL}/data

### Reading results from the result table.
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    "${TABLEDATA_URL}"
## Output:
# {
#  "totalRows": "1",
#  "rows": [
#   {
#    "f": [
#     {
#      "v": "17"
#     }
#    ]
#   }
#  ]
# }

### Setting a timeout value on a query:
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

### Running a query then running jobs.get() to find out the anonymous table name.
QUERY="SELECT 42"
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST  -d "{query:\"${QUERY}\"}" \
    "${QUERIES_URL}"
## Output
# {
# ...
# "jobReference": {
#   "projectId": "bigquery-e2e",
#   "jobId": "job_20db94f7339b4c0084c4522778a9b1d9"
#  }
# }
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json"  \
    -X GET "${JOBS_URL}/job_20db94f7339b4c0084c4522778a9b1d9"
## Output:
# {
# ...
#  "configuration": {
#   "query": {
#    "query": "SELECT 42",
#    "destinationTable": {
#     "projectId": "bigquery-e2e",
#     "datasetId": "_0e32b38e1117b2fcea992287c138bd53acfff7cc",
#     "tableId": "anonde3fd1ade53226f48a842c7518bb9b0fe911e606"
#    },
#    "createDisposition": "CREATE_IF_NEEDED",
#    "writeDisposition": "WRITE_TRUNCATE"
#   }
#  }
# }

### Inspectng the ACL of an anonymous dataset.
ANON_DATASET="_0e32b38e1117b2fcea992287c138bd53acfff7cc"
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X GET "${DATASETS_URL}/${ANON_DATASET}"
## Output:
# {
# ...
#  "access": [
#   {
#    "role": "OWNER",
#    "userByEmail": "jtigani@gmail.com"
#   }
#  ]
# }

### Inspecting an anonymous table.
ANON_TABLE="anonde3fd1ade53226f48a842c7518bb9b0fe911e606"
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X GET \
    "${DATASETS_URL}/${ANON_DATASET}/tables/${ANON_TABLE}"
## Output:
# {
# ...
#  "tableReference": {
#   "projectId": "bigquery-e2e",
#   "datasetId": "_0e32b38e1117b2fcea992287c138bd53acfff7cc",
#   "tableId": "anonde3fd1ade53226f48a842c7518bb9b0fe911e606"
#  },
#  "schema": {
#   "fields": [
#    {
#     "name": "f0_",
#     "type": "INTEGER",
#     "mode": "NULLABLE"
#    }
#   ]
#  },
#  "numBytes": "8",
#  "numRows": "1",
#  "creationTime": "1378396984472",
#  "expirationTime": "1378483384485",
#  "lastModifiedTime": "1378396984472"
# }

### Query cache demonstration.
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST \
    -d "{query:\"${QUERY}\"}" \
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
#   "jobId": "job_7957d818b0a448199ac4b7f539d7d4c2"
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
#  "cacheHit": false
# }

curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST  \
    -d "{query:\"${QUERY}\"}" \
   "${QUERIES_URL}"
## Output:
# {
#  ...
#  "totalRows": "1",
#  "rows": [
#   {
#    "f": [
#     {
#      "v": "42"
#     }
#    ]
#   }
#  ],
#  "totalBytesProcessed": "0",
#  "jobComplete": true,
#  "cacheHit": true
# }

### Running a qury and returning the number of bytes processed:
QUERY='
  SELECT state, count(*) as cnt 
  FROM [bigquery-e2e:reference.zip_codes] 
  WHERE population > 0 AND decommissioned = false 
  GROUP BY state, ORDER BY cnt DESC'
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST  \
    -d "{query:\"${QUERY}\", useQueryCache:false}" \
    "${QUERIES_URL}?fields=totalBytesProcessed"
## Output:
# {
#  "totalBytesProcessed": "552786"
# }

### Running a dry-run query to determine the number of bytes processed
### without actually running the query.
curl  -H "Authorization: Bearer ${AUTH_TOKEN}" \
    -H "Content-Type: application/json" \
    -X POST  \
    -d "{query:\"${QUERY}\", useQueryCache:false, dryRun:true}" \
    "${QUERIES_URL}?fields=totalBytesProcessed"
## Output:
# {
#  "totalBytesProcessed": "552786"
# }



#!/usr/bin/python3
import re
import sys
import io
import json
import boto3
from opensearchpy import OpenSearch, NotFoundError
import urllib
import time
import pyarrow.parquet as pq
from pymongo import MongoClient
from bson import ObjectId
from concurrent.futures import ThreadPoolExecutor
from requests_aws4auth import AWS4Auth
import ujson as json

# S3 objects
s3_client = boto3.client('s3')
downloader = boto3.resource('s3')

region = 'us-west-2'  # Replace with your region
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# documentDB
mongo_client = MongoClient("mongodb://leadfuze:l2e3a9d8F7u6z5e@localhost:27017/?authMechanism=SCRAM-SHA-1&retryWrites=true&w=majority&tls=false&directConnection=true")
mongo_db = mongo_client["prod"]
mongo_coll = mongo_db["visitor_stats"]

# dynamoDB
dynamodb = boto3.resource('dynamodb')
stats_table = dynamodb.Table("visitor_stats_2")

# Customer-bucket mapping
us_states  = [
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY",
  "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
  "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


def extract_date(x):
  pattern = r"y=(\d+)/m=(\d+)/d=(\d+)/h=(\d+)"
  match = re.search(pattern, x)
  if match:
    y, m, d, h = match.groups()
    data = {
       "y": int(y),
       "m": int(m),
       "d": int(d)
    }
    return data
  else: 
    return None

def in_usa(state): return state in us_states

# ES client 
personIdx = "person_v4"
upd_index = "5x5_universal_data"
es = OpenSearch(
    hosts=[{
        "host": "search-leadpool-4ilowbdmqbukskwzzduu37sqyq.us-west-2.es.amazonaws.com",
        "port": 443
    }],
    use_ssl=True,
    verify_certs=True,
    timeout=150,
    retry_on_timeout=True
)


def is_parquet_file(key): return key.endswith(".parquet")

def match_emails_with_lf(emails):
     # Build the query based on the number of emails
    if len(emails) == 1:
        query = {
            "bool": {
                "should": [
                    {"term": {"emails.personal.deliverable": emails[0]}}
                ]
            }
        }
    elif len(emails) > 1:
        query = {
            "bool": {
                "should": [
                    {"terms": {"emails.personal.deliverable": emails}}
                ]
            }
        }
    else:
        return 0, 0, "No emails provided"

    # Execute the search query
    try:
        response = es.search(index=personIdx, body={"query": query})
    except Exception as e:
        print(f"Error executing the search query: {e}")
        return 0, 0, e

    # Parse the response to count total and US-based hits
    total_hits = response['hits']['total']
    us_based_hits = 0

    for hit in response['hits']['hits']:
        source = hit['_source']
        if source.get("location", {}).get("country") == "United States":
            us_based_hits += 1

    return total_hits, us_based_hits, None

def is_json_string(s):
  try:
    json.loads(s)
    return True
  except (ValueError, TypeError):
    return False

def query_upid_in_es(upid):
  try:
    response = es.get(index=upd_index, id=upid)
    return response['_source']
  except NotFoundError:
    return None

def parquet_to_json(file_content):
  table = pq.read_table(io.BytesIO(file_content)).to_pandas()
  return table.to_json(orient="records")

def process_file(key):
  stats = {
    "day": 0,
    "year": 0,
    "month": 0,
    "num_resolved": 0,
    "num_5x5_not_matched": 0,
    "num_upid_found": 0,
    "num_upid_not_found": 0,

    # LF and 5x5 match count
    "num_5x5_us_based_matched": 0,
    "num_5x5_matched": 0,
    "num_lf_matched": 0,
    "num_lf_us_based_matched": 0,
    "num_lf_and_5x5_matched": 0
  }
  is_lf_visitor = False
  s3_object = s3_client.get_object(Bucket=bucket, Key=key)
  file_content = s3_object['Body'].read()  # parquet content
  date = extract_date(key)
  stats["year"] = date.get("y")
  stats["month"] = date.get("m")
  stats["day"] = date.get("d")

  # parquet to json
  parsed = parquet_to_json(file_content)
  parsed = json.loads(parsed)  # list of json

  # Process each record in the parsed data
  for d in parsed:
    parsed = urllib.parse.unquote(d.get("PARTNER_UID"))
    if is_json_string(parsed) is False and parsed == "" or "leadfuze" in parsed: is_lf_visitor = True
    if is_lf_visitor:
      stats["num_resolved"] += 1
      upid = d.get('UP_ID')
      if upid:
        stats["num_upid_found"] += 1
        data = query_upid_in_es(upid)
        if data:
          stats["num_5x5_matched"] += 1
          if in_usa(data["personal_state"]): stats["num_5x5_us_based_matched"] += 1
          emails = []
          if data.get('personal_emails'): emails.append(data['personal_emails'])
          if data.get('additional_personal_emails'): emails.extend(data['additional_personal_emails'].split(","))
          total_hits, us_based_hits, _ = match_emails_with_lf(emails)
          stats["num_lf_matched"]  += total_hits
          stats["num_lf_us_based_matched"] += us_based_hits
        else: stats["num_5x5_not_matched"] +=1
      else: stats["num_upid_not_found"] += 1
  update_stats(stats) # push to mongo

def update_stats(stats):
  # find
  q = {"year": stats.get("year"), "month": stats.get("month"), "day": stats.get("day")}
  projection = {"year": 0, "month": 0, "day": 0, "_id": 0}
  exist_rec = mongo_coll.find_one(q, projection)

  # update
  if exist_rec is not None:
    for key, _ in exist_rec.items():
      if key in stats:
        exist_rec[key] += int(stats[key])
    try: 
      result = mongo_coll.update_one(q, {"$set": exist_rec})
      if result.matched_count > 0: print("Updated in 'visitor_stats'")
    except Exception as e:
      print("Exception while updating", e)
  else: 
    try:
      result = mongo_coll.insert_one(stats)
      if result.inserted_id is not None: 
        print("Inserted in 'visitor_stats'")
        stats.pop("_id")
    except Exception as e:
      print("Exception while inserting", e)


def main(bucket, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    with ThreadPoolExecutor(max_workers=10) as executor:
      for page in pages:
        # print("page: ", page)
        # print()
        # sys.exit()
        keys = [record["Key"] for record in page.get("Contents", []) if is_parquet_file(record["Key"])]
        # keys = []
        # for record in page.get("Contents", []): 
        #   if is_parquet_file(record["Key"]):
        #     keys.append(record["Key"])
      # for k in keys: process_file(k)
        executor.map(process_file, keys)

if __name__ == "__main__":
  bucket = "trovo-coop-leadfuze"
  prefix = "outgoing/cookie_sync/resolved/"

  st  = time.time()
  main(bucket, prefix)
  diff = time.time() - st
  minutes, seconds = divmod(diff, 60)

  print("stats")
  # print(json.dumps(stats, indent=2))
  print("diff: {} minutes and {:.3f} seconds".format(int(minutes), seconds))
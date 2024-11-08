#!/usr/bin/python3
import os
from collections import defaultdict
import streamlit as st
import pandas as pd
import boto3
from datetime import date
from pymongo import MongoClient
from boto3.dynamodb.conditions import Key


# documentDB
mongo_client = MongoClient("mongodb://leadfuze:l2e3a9d8F7u6z5e@localhost:27017/?authMechanism=SCRAM-SHA-1&retryWrites=true&w=majority&tls=false&directConnection=true")
mongo_db = mongo_client["prod"]
mongo_coll = mongo_db["visitor_stats"]

mapping = {
	"num_resolved": "Number of resolved",
	"num_lf_matched": "Matched with Leadfuze",
	"num_5x5_matched": "Matched with 5x5",
	"num_5x5_not_matched": "Not matched with 5x5", 
	"num_lf_us_based_matched": "Matched with Leadfuze (US only)",
	"num_5x5_us_based_matched": "Matched with 5x5 (US only)",
	"num_lf_5x5_matched": "Matched with Leadfuze and 5x5",
	"num_upid_found": "Number of UPID found",
	"num_upid_not_found": "Number of UPID not found",
	"num_lf_and_5x5_matched": "Matched with Leadfuze and 5x5"
}

st.set_page_config(layout="wide")

access_key = os.environ["AWS_ACCESS_KEY_ID"]
access_secret = os.environ["AWS_SECRET_ACCESS_KEY"]

dynamodb = boto3.resource(
	'dynamodb',
	aws_access_key_id=access_key,
	aws_secret_access_key=access_secret,
	region_name="us-west-2"
)
table = dynamodb.Table('visitor_stats')

def fetch_data(fd, td):
	query = {
		"$or": []
	}
	cd = {"year": date.today().year, "month": date.today().month, "day": date.today().day}

	if fd is not None and td is not None:
		query["$or"].append(fd)
		query["$or"].append(td)
		query["$or"].append({"year": {"$gt": fd.get("year"), "$lt": td.get("year")}})
		query["$or"].append({"year": fd.get("year"), "month": fd.get("month")})
		query["$or"].append({"year": td.get("year"), "month": td.get("month")})
	elif fd is not None and td is None:  
		query["$or"].append(fd)
		query["$or"].append(cd)
		query["$or"].append({"year": {"$gt": fd.get("year"), "$lt": td.get("year")}})
		query["$or"].append({"year": fd.get("year"), "month": fd.get("month")})
		query["$or"].append({"year": cd.get("year"), "month": cd.get("month")})
	elif fd is None and td is not None:
		query["$or"].append(cd)
		query["$or"].append(td)
		query["$or"].append({"year": {"$gt": cd.get("year"), "$lt": td.get("year")}})
		query["$or"].append({"year": cd.get("year"), "month": cd.get("month")})
		query["$or"].append({"year": td.get("year"), "month": td.get("month")})

	proj = {"_id": 0, "day": 0, "year": 0, "month": 0}
	docs = mongo_coll.find(query, proj)
	return docs

# Streamlit App
st.title("Leadfuze visitor stats")
col1, col2, _ = st.columns([5,5, 1]) 
with col1:
	from_date = st.date_input("From", date.today(), max_value=date.today())
	fd = {
		"year": from_date.year,
		"month": from_date.month,
		"day": from_date.day
	}

with col2:
	to_date = st.date_input("To", date.today(), max_value=date.today())
	td = {
		"year": to_date.year,
		"month": to_date.month,
		"day": to_date.day
	}

if st.button("Get Visitor Stats", "btn"):
	data = fetch_data(fd, td)
	# df = pd.DataFrame(data)
	# st.write(df)
	res = defaultdict(int)
	for d in data:
		for key, value in d.items():
			if key not in mapping:
				print(key)
			res[mapping.get(key)] += value
			# st.write(f"**{mapping.get(key)}**: {value}")

	df = pd.DataFrame([res])
	st.write(df)
	# if data:
	# 	for key, value in data[0].items():
	# 		if "id" not in key:
	# 			st.write(f"**{mapping.get(key)}**: {value}")
	# else:
	# 	st.write("No visitor data found")

# with col3:

# # Fetch data
# data = fetch_data(fd, td)

# if data:
# 	for key, value in data[0].items():
# 		if "id" not in key:
# 			st.write(f"**{mapping.get(key)}**: {value}")
# else:
# 	st.write("No visitor data found")
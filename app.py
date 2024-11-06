#!/usr/bin/python3
import os
import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key

mapping = {
	"num_resolved": "Number of resolved",
	"num_lf_matched": "Matched with Leadfuze",
	"num_5x5_matched": "Matched with 5x5",
	"num_5x5_not_matched": "Not matched with 5x5", 
	"num_lf_us_based_matched": "Matched with Leadfuze (US only)",
	"num_5x5_us_based_matched": "Matched with 5x5 (US only)",
	"num_lf_5x5_matched": "Matched with Leadfuze and 5x5"
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

def fetch_data():
	# Query the table where id == "stats"
	response = table.query(
		KeyConditionExpression=Key('id').eq('stats')
	)
	return response.get('Items', [])

# Streamlit App
st.title("Leadfuze visitor stats")

# Fetch data
data = fetch_data()

if data:
	for key, value in data[0].items():
		if "id" not in key:
			st.write(f"**{mapping.get(key)}**: {value}")
else:
	st.write("No visitor data found")

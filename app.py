#!/usr/bin/python3
import os
import streamlit as st
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key

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
st.title("Visitor Stats Dashboard")

# Fetch data
data = fetch_data()

if data:
	for key, value in data[0].items():
		if "id" not in key:
			st.write(f"**{key}**: {value}")
else:
	st.write("No visitor data found")

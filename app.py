#!/usr/bin/python3
import os
from collections import defaultdict
import streamlit as st
import pandas as pd
import boto3
from datetime import date, datetime
from pymongo import MongoClient

PASSWORD = "LEADFUZE"

# documentDB
mongo_client = MongoClient("mongodb://leadfuze:l2e3a9d8F7u6z5e@localhost:27017/prod?authMechanism=SCRAM-SHA-1&retryWrites=true&w=majority&tls=false&directConnection=true")
mongo_db = mongo_client["prod"]
mongo_coll = mongo_db["visitor_stats"]
profile_pic_coll = mongo_db["profilePicStats"]

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

def fetch_data(fd, td, client):
    if not client:
        raise ValueError("Client key is required")

    # Use current date as default if fd or td is None
    cd = {"year": date.today().year, "month": date.today().month, "day": date.today().day}
    fd = fd or cd
    td = td or cd


    # Create datetime objects for the 'from' and 'to' dates
    from_date = datetime(fd["year"], fd["month"], fd["day"])
    to_date = datetime(td["year"], td["month"], td["day"])

    query = {
        "$and": [
            {"client": client},
            {"date": {"$gte": from_date, "$lte": to_date}}
        ]
    }

    proj = {"_id": 0, "day": 0, "year": 0, "month": 0}
    docs = mongo_coll.find(query, proj)
    return docs

def fetch_profile_pic_stats():
    result = profile_pic_coll.find_one({'_id': 1})
    return result



def visitor_stats_section(title, button_key, client_key):
    st.title(title)

    col1, col2, _ = st.columns([5, 5, 1])
    with col1:
        from_date = st.date_input("From", date.today(), max_value=date.today(), key=f"From {client_key}")
        fd = {
            "year": from_date.year,
            "month": from_date.month,
            "day": from_date.day,
        }

    with col2:
        to_date = st.date_input("To", date.today(), max_value=date.today(), key=f"To {client_key}")
        td = {
            "year": to_date.year,
            "month": to_date.month,
            "day": to_date.day,
        }

    if st.button(f"Get Visitor Stats", key=button_key):
        data = fetch_data(fd, td, client_key)
        if not data:
            st.warning(f"No data found for {client_key} in the selected date range.")
            return

        res = defaultdict(int)
        for d in data:
            for key, value in d.items():
                if key in mapping:
                    res[mapping[key]] += value       
        df = pd.DataFrame(list(res.items()), columns=["Metric", "Value"])
        st.write(df)

def profile_pic_stats_section():
    data = fetch_profile_pic_stats()
    st.title('Profile Pics Stats')

    res = defaultdict(int)
    res["Total Requests"] = data["total"]
    res["Profile Pic Resolved"] = data["resolved"]
    res["Resolved for Brandwell"] = data["brandwell"]
    res["Resolved for Leadfuze"] = data["leadfuze"]

    df = pd.DataFrame(list(res.items()), columns=["Metric", "Value"])
    st.write(df)

    dt = datetime.fromtimestamp(data["created_at"] / 1000.0)
    tracked_from = dt.strftime('%Y-%m-%d %H:%M:%S')

    dt = datetime.fromtimestamp(data["modified_at"] / 1000.0)
    last_refreshed = dt.strftime('%Y-%m-%d %H:%M:%S')
    st.markdown(f"**From Date:** {tracked_from}")
    st.markdown(f"**To Date:** {last_refreshed}")

def start_app():
    col1, col2 = st.columns(2)
    with col1:
        visitor_stats_section("Leadfuze Visitor Stats", "btn_leadfuze", "leadfuze")
    with col2:
        visitor_stats_section("Brandwell Visitor Stats", "btn_brandwell", "brandwell")
    profile_pic_stats_section()

# Streamlit App
if 'auth' not in st.session_state:
	st.session_state.auth = False

password = st.text_input("Enter password", type="password")
if st.button("Validate Password", "passBtn"):
	if password == PASSWORD:
		st.session_state.auth = True
	else: 
		st.warning("Incorrect password")

if st.session_state.auth:
	start_app()

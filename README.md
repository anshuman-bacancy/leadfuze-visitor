## How to update the code  
Copy the file from local to EC2 instance 
```bash
scp -i anshuman-lf-key.pem -r <file-path> ubuntu@ec2-35-91-48-36.us-west-2.compute.amazonaws.com:~/
```

### Start virtual env and install the dependencies
ssh into the EC2 instance and perform the following steps.
```bash
python3 -m venv myenv
source myenv/bin/activate
pip3 install -r requirements.txt
```

## Running the script

### get stats
```bash
./stats.py OR python3 stats.py
```

### start streamlit app local
```bash
streamlit run ./app.py OR streamlit run app.py
```

### start streamlit app on EC2
```bash
cd stats
nohup streamlit run app.py --server.port 8501 &
```

### Live app
[streamlit app](http://35.91.48.36:8501/)

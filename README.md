## How to update the code  
manually copy the file from local to EC2 instance 
* update the code in local
```bash
scp -i anshuman-lf-key.pem -r <file-path> ubuntu@ec2-35-91-48-36.us-west-2.compute.amazonaws.com:~/
```
* Running the streamlit app in EC2 instance
```bash
cd stats
nohup streamlit run app.py --server.port 8501 &

```

## Install the dependencies: 
```bash
pip3 install -r requirements.txt
```
## Running the script
### get the stats
```bash
./stats.py OR python3 stats.py
````

### run the streamlit app locally
```bash
streamlit run ./app.py OR streamlit run app.py
````
[streamlit app](ec2-35-91-48-36.us-west-2.compute.amazonaws.com:3501)


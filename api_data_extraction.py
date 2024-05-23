import requests
import json
import configparser
import csv
import boto3
from datetime import datetime, tzinfo


#grap the response from the api (its in json format)
api_response = requests.get("http://api.open-notify.org/iss-now.json")

#create json object from the response content
response_json = json.loads(api_response.content)


#create a list for each spaceships that passes
all_passes = []

#loop through each space craft?
#for response in response_json:
#  print(response_json[response])


current_pass = []


current_pass.append(response_json['message'])

#store the lat/log from the request
current_pass.append(response_json['iss_position']['latitude'])
current_pass.append(response_json['iss_position']['longitude'])

#get timestamp, but its in unix epoch
dateUnix = int(response_json['timestamp'])

#convert to readable format
dateReadable = datetime.fromtimestamp(dateUnix).strftime('%Y-%m-%d %H:%M:%S')
current_pass.append(dateReadable)

#append current pass to our mega list
all_passes.append(current_pass)

#write our results to the file
export_file = "ISS_location.csv"
with open(export_file, 'w') as fp:
	csvw = csv.writer(fp, delimiter=',')
	csvw.writerows(all_passes)

fp.close()



import csv
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import boto3
import pymysqlreplication
import time
"""
MySQL binlog is a log that keeps a record of every operation (INSERT, DELETE, UPDATE)
performed in the database. The Purpose is to replicate the data and ingest it into a data warehouse.
binlog_reader connects to MySQl database, reads the binlog and writes to a local file
called orders_extract.csv and then sends it S3 in AWS. 
"""

#create parser object
parser = configparser.ConfigParser()
parser.read("pipeline.conf")

#read credentials
hostname = parser.get("mysql_config" , "hostname")
port = parser.get("mysql_config" , "port")
username = parser.get("mysql_config" , "username")
dbname = parser.get("mysql_config" , "database")
password = parser.get("mysql_config" , "password")

#create dictionary of sql settings
mysql_settings = {
  "host": hostname,
  "port": int(port),
  "user": username,
  "password": password
}

#create bin log stream reader object
#first connects to mysql 
try:
  b_stream = BinLogStreamReader(
    connection_settings=mysql_settings,
    server_id=1000,
    #blocking=True,
    #resume_stream= True,
    log_pos=1,
    only_events=[row_event.DeleteRowsEvent,
                 row_event.WriteRowsEvent,
                 row_event.UpdateRowsEvent]

  )
except Exception as e:
  print(f"Error connecting to MySQL: {e}")


#createa list of order events to be placed in csv
order_events = []
print(order_events)
#for each event of our bin log
for binlogevent in b_stream:
  print(binlogevent)
  #parse through each row
  for row in binlogevent.rows:

    #we will parse through each table of the DB
    #if the table is orders
    if binlogevent.table == 'orders':
      
      #create a dictionary to store the type of action from the bin log
      event = {}
      
      #parse through each instance : delete , update, and insert
      if isinstance(binlogevent,row_event.DeleteRowsEvent):
        event["action"] = "delete"
        event.update(row["values"].items())
      elif isinstance(binlogevent,row_event.UpdateRowsEvent):
        event["action"] = "update"
        event.update(row["after_values"].items())
      elif isinstance(binlogevent,row_event.WriteRowsEvent):
        event["action"] = "insert"
        event.update(row["values"].items())

      #append to our dictionary
      order_events.append(event)

#close our stream
b_stream.close()

print(order_events)
keys = order_events[0].keys()

#we will save bin log dictionary into an orders_extract file
local_filename = 'orders_extract.csv'

#open our filename and write and write the keys
with open(local_filename,'w', newline='') as output_file:
    dict_writer = csv.DictWriter(
                output_file, keys,delimiter='|')
    
    #write the events to each row
    dict_writer.writerows(order_events)

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials","access_key")
secret_key = parser.get("aws_boto_credentials","secret_key")
bucket_name = parser.get("aws_boto_credentials","bucket_name")

#create s3 bucket object with credenitals
s3 = boto3.client('s3',
                  aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

#copy local filename 
s3_file = local_filename

#upload our file to s3 bucket
s3.upload_file(local_filename,bucket_name, s3_file)

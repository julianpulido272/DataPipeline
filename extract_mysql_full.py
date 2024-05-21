import pymysql
import csv
import boto3
import configparser
"""
extract_mysql_full is a script designed to connect to a MySql database using your credentials
and then print out the database content into a csv file to your local machine. It makes a connection
to AWS and uploads the file to an S3 bucket for further data loading/transferring.
"""
#initiliaze connection to mysql DB

#create parser object
parser = configparser.ConfigParser()
parser.read("pipeline.conf")

#read credentials
hostname = parser.get("mysql_config" , "hostname")
port = parser.get("mysql_config" , "port")
username = parser.get("mysql_config" , "username")
dbname = parser.get("mysql_config" , "database")
password = parser.get("mysql_config" , "password")

conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=int(port))

if conn is None:
  print("Error connecting to the MySQL database")
else:
  print("MySQL connection established successfully")


#Run full extraction of the Orders table
m_query = "SELECT * FROM Orders;"

#filename to save query output to
local_filename = "order_extract.csv"

#create new cursor to execute queries with
m_cursor = conn.cursor()

#execute query
m_cursor.execute(m_query)

#fetch all rows
results = m_cursor.fetchall()

#write to our file
with open(local_filename, 'w') as fp:
  csv_w = csv.writer(fp, delimiter=',')
  csv_w.writerows(results)

#close everything
fp.close()
m_cursor.close()
conn.close()

#AWS bucket credentials

#read the pipeline.conf to get configurations
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

#create s3 bucket object with credenitals
s3 = boto3.client('s3',
                  aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

#copy local filename 
s3_file = local_filename

#upload our file to s3 bucket
s3.upload_file(local_filename,bucket_name, s3_file)



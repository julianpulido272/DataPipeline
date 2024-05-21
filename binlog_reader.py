from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import row_event
import configparser
import pymysqlreplication

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
  "passwd": password
}

#create bin log stream reader object
#first connects to mysql 
b_stream = BinLogStreamReader(
  connection_settings=mysql_settings,
  server_id=100,
  resume_stream= True,
  log_pos=1400,
  only_events=[row_event.DeleteRowsEvent,
               row_event.WriteRowsEvent,
               row_event.UpdateRowsEvent]

)

print(b_stream.mysql_version)
for event in b_stream:
  event.dump()

b_stream.close()


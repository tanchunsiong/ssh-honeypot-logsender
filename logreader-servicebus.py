#<standard shebang line>
#fixed the import, just red PEP 8
import sys
import re
import time
import datetime
import os
import threading
import json
from multiprocessing import Pool
from azure.servicebus import ServiceBusClient
from azure.servicebus import Message

sb_name= "yourservicebusname"
key_name = "yourkeyname"
key_value = "yoursecretkeyvalue"
queue_name="yourqueuename"

sb_client = ServiceBusClient.from_connection_string("Endpoint=sb://"+sb_name+".servicebus.windows.net/;SharedAccessKeyName="+key_name+";SharedAccessKey="+key_value+"")

#try to create the queue
try:
    sb_client.create_queue(queue_name)
except:
    print "Unexpected error:",sys.exc_info()[0]

sbs=sb_client.get_queue("MyQueue")

try:
    if sys.argv[1:]:
        print "File: %s" % (sys.argv[1])
        logfile = sys.argv[1]
        
    else:
        #logfile = raw_input("Please enter a file to parse, e.g /var/log/secure: ")
        logfile ="passwords.json"


    #infinite loop
    while(True):
        lastRunWaterMark=datetime.datetime.now()
        thisRunWaterMark=datetime.datetime.now()
       
        #reset the counters
        isfirstrun=0
        isfirstrunRepeat=1
        count=0

        #get last watermark, if no watermark exist, this is new run
        try:
            timefile = open("last_time_processed", "r")   
          
            for text in timefile.readlines():
                    lastRunWaterMark=datetime.datetime.strptime(text,"%Y-%m-%dT%H:%M:%S.%f")
        except:
             #lastRunWaterMark =datetime.datetime.now()
             isfirstrun=1

        #try to read the log file
        try:

            file = open(logfile, "r")

            #read the last 20 lines only
            for text in reversed(file.readlines()):
               if count >=20:
                   break
               try:
                   text = text.rstrip()
                   text_dict = json.loads(text)
                   timecreated= datetime.datetime.strptime(str(text_dict['timestamp']),"%Y-%m-%dT%H:%M:%S.%f")
                   if isfirstrun == 1 :
                       if isfirstrunRepeat == 1 :
                           thisRunWaterMark=timecreated
                           isfirstrunRepeat=0
                       #greedy run first
                       outfile = open("last_time_processed", "w")
                       outfile.write(thisRunWaterMark.strftime("%Y-%m-%dT%H:%M:%S.%f"))
                       outfile.close()
                       #run until end 
                       if 1==1 :
                        count+=1 
                        print('sending event at ' +text)
                        sbs.send(Message(text))
                    #item is new, proceed to send
                   elif timecreated  > lastRunWaterMark:
                      if isfirstrunRepeat == 1 :
                          #assign to last row executed
                          thisRunWaterMark=timecreated
                          isfirstrunRepeat=0
                      if 1==1 :
                        count+=1
                        print('sending event at ' +text)
                        sbs.send(Message(text))
                   else:
                       break #exit loop
               except:
                     print "Unexpected error:",sys.exc_info()[0]
        except:
            print "Unexpected error:",sys.exc_info()[0]    


        finally:
            file.close
            outfile = open("last_time_processed", "w")
            outfile.write(thisRunWaterMark.strftime("%Y-%m-%dT%H:%M:%S.%f"))
            outfile.close()
        if count >= 1 :
            print("Processed "+ str(count) +" items from "+ lastRunWaterMark.strftime("%Y-%m-%dT%H:%M:%S")  + " to " + thisRunWaterMark.strftime("%Y-%m-%dT%H:%M:%S"))
			#print("Sleeping for 5 seconds at " + thisRunWaterMark.strftime("%Y-%m-%dT%H:%M:%S"))
        time.sleep(5)
except IOError, (errno, strerror):
        print "I/O Error(%s) : %s" % (errno, strerror)

def sendToAzure(messagetosend):
    sbs.send(messagetosend)
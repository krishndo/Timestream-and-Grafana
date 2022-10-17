import boto3
import json
from datetime import datetime, timedelta
import math
import time
import random

# constants
TOPIC = 'timestream_test'
DAYS = 30
SAMPLES_PER_DAY = 24
SAMPLE_PERIOD = 24 / SAMPLES_PER_DAY * 60
HOURS_FROM_UTC = -8
LOCATION = ['location1', 'location2']
TEMP_MEDIAN = 72
HUMIDITY_MEDIAN = 50
PRESSURE_MEDIAN = 29
AMPLITUDE = 1

# get the ATS endpoint
def GetIoTEndpoint():
    IoTClient = boto3.client('iot')
    endpoint = IoTClient.describe_endpoint(endpointType = 'iot:Data-ATS')
    return endpoint

# generate data centered at median with a sine wave of amplitude 
def GenerateData(currentSamplePoint, deviceID):
    data = {}

    rads = 2 * math.pi * currentSamplePoint / SAMPLES_PER_DAY
    # offset one set of data by 90 degrees, so the data plots don't completely obscure each other
    if deviceID == 1:
        rads += math.pi / 2
    offset = AMPLITUDE * math.sin(rads) + random.randrange(1, 5) # add some randomness as well
    offset += 0.001 # to ensure double type
    data['temperature'] = TEMP_MEDIAN + offset
    data['humidity'] = HUMIDITY_MEDIAN + offset
    data['pressure'] = PRESSURE_MEDIAN + offset
    return data

# generate metadata and timestamps
def GeneratePayload(messageTime, deviceID, location, currentSamplePoint):
    payloadDict = {}
    # add a second to the second device's timestamp so the timestamp for each device are not the same
    if (deviceID == 1):
        messageTime += timedelta(seconds = 1)
    # simple way to calculate the time from the epoch, without using timezones, funky libraries, etc.
    # this is in seconds
    timestamp = (messageTime - datetime(1970, 1, 1)) / timedelta(seconds = 1)
    # multiply by a billion for nanoseconds
    timestamp *= 1e9
    # get measurements
    data = GenerateData(currentSamplePoint, deviceID)
    # timestamp needs to be a whole number - timestamps don't like 1.23456+18
    payloadDict['timestamp'] = int(timestamp)
    # pass a copy of the local time because I can't convert nanosecond UTC timestamps into local time in my head
    payloadDict['local_time'] = (messageTime + timedelta(hours = HOURS_FROM_UTC)).isoformat()
    # a couple more pieces of metadata
    payloadDict['deviceID'] = deviceID
    payloadDict['location'] = location
    # actual measurements inserted
    payloadDict['temperature'] = data['temperature']
    payloadDict['pressure'] = data['pressure']
    payloadDict['humidity'] = data['humidity']
    # get the dictionary in a string
    payloadString = json.dumps(payloadDict)
    # convert the string to a byte array to use boto3 publish
    payloadArray = payloadString.encode(encoding = 'utf-8')
    return payloadArray

# be sure to use the ATS endpoint
endpoint = GetIoTEndpoint()
IoTDataClient = boto3.client('iot-data', endpoint_url = 'https://' + endpoint['endpointAddress'])
# get the current UTC time
currentTime = datetime.utcnow()
# start the mqtt messages x days in the past
messageTime = currentTime - timedelta(days = DAYS)
# sample point is used to determine the angle of each measurement - want to see a sine wave in the data,
# so you can see the data 'correctly' while viewing it - random data is, well, random
# one complete sine wave every day
currentSamplePoint = 1
# send two messages to represent two different devices
count = 1 # should start at 0 but this leads to the first sets of numbers sent to Timestream being 
          # like xx.0, which sets up the columns as bigint and not double
          # all subsequent writes convert the data from double to int and gets rid of all precision
while messageTime < currentTime:
    for i in range(2):
        payload = GeneratePayload(messageTime, i, LOCATION[i], currentSamplePoint)
        IoTDataClient.publish(topic = TOPIC, qos = 1, payload = payload)
        print('added[{0}]: {1}'.format(count, payload))
        count += 1
    # move to the next sample time
    messageTime += timedelta(minutes = int(SAMPLE_PERIOD))
    # need a modulo counter that goes from [0,number of samples per day)
    currentSamplePoint += 1
    currentSamplePoint %= SAMPLES_PER_DAY
    # pause before sending next sample
    time.sleep(1)
import json
from datetime import datetime
from time import time

# from pymongo import MongoClient

# client = MongoClient("mongodb://localhost:27017/")
# db = client["firefighting-production-new"]
# infos = db["info"].find({})
partCode2partType = {}
count = 0
# for item in infos:
#     for data in item["datas"]:
#         count += 1
#         partCode2partType[data["partCode"]] = data["partType"]
# print(len(partCode2partType), count)

import mysql.connector

cnx = mysql.connector.connect(user="root", host="127.0.0.1", database="bdp_info")
sensor_count = {}
cursor = cnx.cursor()
query = "SELECT sensor_id, sensor_type,building_id FROM bdp_info.tmp_sensor"
cursor.execute(query)
for sensor_id, sensor_type, building_id in cursor:
    count += 1
    partCode2partType[sensor_id] = sensor_type
print(len(partCode2partType), count)


in_count = 0
missing_count = 0
no_event_count = 0
postTime = ""
insert_list = []
avail = 0
fn = "1202_1207.log"
json_time = 0.01
parse_time = 0.01
print("counting lines")
num_lines = sum(1 for _ in open(fn))
for i, line in enumerate(open(fn)):
    print(
        f"{i / num_lines:.4f}",
        i,
        num_lines,
        f"{in_count / max(i - 1050330,1):.4f}",
        in_count,
        missing_count,
        no_event_count,
        postTime,
        f"{json_time / parse_time:.4f}",
        end="\r",
    )
    avail = 0
    # if i > 1050330:
    #     print("")
    #     exit(0)
    t1 = time()
    try:
        data = json.loads(line)
    except:
        continue
    t2 = time()
    # data = parser.parse_string(line)
    data = data["data"]
    partCode = data["data"]["device_id"]
    postTime = data["postTime"]
    # time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    # time = datetime.strftime(time, "%Y-%m-%dT%H:%M:%S.000Z")
    partType = partCode2partType.get(partCode)
    # print(partType)
    if partType is None:
        missing_count += 1
        avail = 1
        continue
    if partType == 1:  # and data["data"]["status"] == 6:
        data["data"]["event_type"] = "1"
    elif data["data"].get("faultState") == "1" or (
        partType in [1, 10] and data["data"].get("status") == "3"
    ):
        data["data"]["event_type"] = "2"
    else:
        if "event_type" not in data["data"]:
            no_event_count += 1
            avail = 2
            continue
    realTime = datetime.strptime(postTime, "%Y-%m-%d %H:%M:%S")
    res = {
        "partType": partType,
        "maint": 0,
        "partCode": partCode,
        # "time": realTime,
        "time": postTime,
        "fireAlarm": 0,
        "errorStatus": 0,
        "actionStatus": 0,
        "algoType": 0,
        "algoStatus": 1,
    }
    if data["data"]["event_type"] == "1":
        res["algoType"] = 100
    if data["data"]["event_type"] == "2":
        res["algoType"] = 200
    insert_list.append(res)
    in_count += 1
    avail = 0
    t3 = time()
    json_time += t2 - t1
    parse_time += t3 - t2
    # print(res)
    # req = requests.post("http://127.0.0.1:8000/api/v1/data/status", json=res)
    # partCode = data["device_id"]
    # print(data)
print("")
print(len(insert_list))
json.dump(insert_list, open("insert_list.json", "w"), ensure_ascii=False, indent=4)

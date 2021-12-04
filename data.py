import json
from datetime import datetime

insert_list = json.load(open("./insert_list.json"))


from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["firefighting-production-new"]
partCode2projID = dict()
infos = list(db["info"].find({}))
for info in infos:
    for data in info["datas"]:
        partCode2projID[data["partCode"]] = info["projID"]
print(len(partCode2projID))

res = {}
for item in insert_list:
    time = datetime.strptime(item["time"], "%Y-%m-%d %H:%M:%S")
    date = time.date()
    algoType = item["algoType"]
    partCode = item["partCode"]
    projID = partCode2projID[partCode]
    if projID not in res:
        res[projID] = {}
    res_item = res[projID]
    if algoType not in res_item:
        res_item[algoType] = {}
    res_item_item = res_item[algoType]
    res_item_item[partCode] = res_item_item.get(partCode, 0) + 1
print(res)
json.dump(res, open("res.json", "w"), ensure_ascii=False, indent=4)

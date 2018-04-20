from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import os

# Read Only
# e0t3rx
# 194122..

# Read + Write
# superadmin
# mz^t93FGcPufwjSz

def log(message):
    logger_path = "/".join(os.path.realpath(__file__).split("/")[:-1] + ["logger.log"])
    with open(logger_path, "a+") as f:
        f.write(message)
        f.write("\n" * 5)


consumer = KafkaConsumer(
            "hpot_H1",
            bootstrap_servers="35.184.35.37:9092"#,
            #auto_offset_reset='smallest'
            )

uri = "mongodb://superadmin:mz^t93FGcPufwjSz@localhost/hpots?authSource=hpots"
client = MongoClient(uri)
db = client['hpots']
collection = db['H1']

for msg in consumer:
    try:
        dict_msg = json.loads(msg.value.decode())
        # Store the dict formatted msg into MongoDB
        collection.insert(dict_msg)
    except Exception as e:
        log("Something went wrong: " + str(e) + "\n Error Message: " + str(msg.value.decode()))

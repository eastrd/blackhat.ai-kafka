from kafka import KafkaProducer
from datetime import datetime
from time import sleep
import os


def log(message):
    logger_path = "/".join(os.path.realpath(__file__).split("/")[:-1] + ["logger.log"])
    with open(logger_path, "a+") as f:
        f.write(message)
        f.write("\n" * 5)

        

file_path = "/home/cowrie/cowrie/log/cowrie.json"
kafka_broker = "35.184.35.37:9092"
topic = "hpot_H1"


get_today = lambda: datetime.now().strftime("%Y-%m-%d")
today = get_today()

get_today = lambda: datetime.now().strftime("%Y-%m-%d")
today = get_today()

producer = KafkaProducer(bootstrap_servers = kafka_broker)



def main():
    global today
    with open(file_path, "r") as f:
        while True:
            if get_today() != today:
                # Exit condition: Date changed
                exit()
            line = f.readline().replace("\n", "")
            if len(line) > 0:
                # Forward the line as message to Kafka
                # print("Sending: "+line)
                producer.send(topic, line.encode())
                producer.flush()
            else:
                sleep(5)

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            log("Something went wrong: " + str(e))
            sleep(5)

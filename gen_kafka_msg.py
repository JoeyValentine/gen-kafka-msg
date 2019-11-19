import json
import multiprocessing as mp
import os
import random
import string
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


def send_messages(period: float, bootstrap_servers: list, topic: str) -> None:

    def gen_random_str(str_len: int) -> str:
        return ''.join(random.choice(string.ascii_lowercase) for i in range(str_len))

    def gen_random_msg(start_id: int, end_id: int, str_len: int) -> list:
        msg = {"id" : random.randint(start_id, end_id),
                "COMP_SEQNO" : gen_random_str(str_len),
                "BLOCK_NO" : gen_random_str(str_len),
                "MODULE_NO" : gen_random_str(str_len),
                "HEAD_ID" : gen_random_str(str_len),
                "HOLDER_NO" : gen_random_str(str_len),
                "NOZZLE_ID" : gen_random_str(str_len),
                "PART_NO" : gen_random_str(str_len),
                "REEL_ID" : gen_random_str(str_len),
                "FEEDER_ID" : gen_random_str(str_len),
                "REFERENCE" : gen_random_str(str_len),
                "DATA_CODE" : gen_random_str(str_len),
                "PICKUP_STATUS" : gen_random_str(str_len)}
        return msg

    random.seed(os.getpid())

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks='all', \
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))

    while True:
        # Assume that 1 <= id <= 10000000
        msg = gen_random_msg(1, 10000000, 10)
        future = producer.send(topic, msg)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as err:
            print(err)

        print(f"pid : {os.getpid()} send : {msg}\n")
        time.sleep(period)

    producer.flush()
    producer.close()


if __name__ == '__main__':
    bootstrap_servers = ['IP:PORT']
    n_processes = 5
    period = 1
    topic = 'topic_name'

    for i in range(n_processes):
        p = mp.Process(target=send_messages, args=(period, bootstrap_servers, topic))
        p.start()

    p.join()


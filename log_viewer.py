import time
from kafka import KafkaProducer


def producer(line_str):
    try:
        ln = bytes(line_str, 'utf-8')
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.send('system', ln)
        print('Successfully sent log to kafka')
        producer.flush()
        producer.close()
    except Exception as e:
        print(e)
        print('Fail to senf log')



try:
    def follow(file):
        while 1:
            where = file.tell()
            line = file.readline()
            if not line or line == '\n':
                time.sleep(1)
                file.seek(where)
            else:
                producer(line)
                print('Trying sent log to kafka topic system...')

    with open('/var/log/messages', 'r') as log_file:
        log_file.seek(0, 2)
        follow(log_file)

except Exception as e:
    print(e)

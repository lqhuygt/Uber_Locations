import time, csv
from json import dumps
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class  MyHandler(FileSystemEventHandler):

    def  on_modified(self,  event):
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Event type: {event.event_type} path : {event.src_path}")

    def  on_deleted(self,  event):
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Event type: {event.event_type} path : {event.src_path}")

    def  on_created(self,  event):
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Event type: {event.event_type} path : {event.src_path}")
        
        TOPCIC_NAME = "StreamUber"
        BOOTSTRAP_SERVER = "localhost:9092"
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER,value_serializer=lambda x:dumps(x).encode('utf-8'))

        with open(event.src_path) as file:
            column = ['dt', 'lat', 'lon','base']
            data = csv.DictReader(file, delimiter=",", fieldnames=column)
            for row in data:
                producer.send(TOPCIC_NAME,row)
            

if __name__ ==  "__main__":
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler,  path="/Users/huyle/Desktop/ftp",  recursive=True)
    observer.start()

    try:
        while  True:
            time.sleep(1)
    except  KeyboardInterrupt:
        observer.stop()
    observer.join()

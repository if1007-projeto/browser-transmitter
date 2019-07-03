import argparse
from flask import Flask, Response
from kafka import KafkaConsumer
import time

max_attempts = 1000

app = Flask(__name__)

def get_command_line_arguments():
    parser = argparse.ArgumentParser(
        description='Browser Transmitter')
    
    parser.add_argument('-u', '--kafka-url', dest='kafka_url', metavar='<url>', required=True, type=str,
                        help='kafka url. Example: localhost:9092')

    parser.add_argument('-t', '--kafka-topic', dest='kafka_topic', metavar='<kafka topic>', required=True, type=str,
                        help='kafka topic to subscribe and read frames')

    args = parser.parse_args()

    return args

def try_connect_kafka():
    print('Trying to connect to Kafka - url: %s, topic %s' % (url, topic))
    for attempt in range(max_attempts):
        print('Attempt to connect to Kafka - %d tries' % attempt)
        try:
            consumer = KafkaConsumer(topic, bootstrap_servers=[url])
            return consumer
        except:
            print('Error connecting')
        
        time.sleep(1)

@app.route('/video', methods=['GET'])
def video():
    return Response(
        get_video_stream(),
        mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream():
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + msg.value + b'\r\n\r\n')

args = get_command_line_arguments()

topic = args.kafka_topic
url = args.kafka_url

consumer = try_connect_kafka()

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
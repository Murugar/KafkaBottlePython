from bottle import route, run, template
from kafka import KafkaConsumer

@route('/')
def home():
    return template("Welcome {{string}}", string="Home")

@route('/hello')
def hello():
    return template("Hello {{string}}", string="World")


@route('/kafka')
def consumer():
    consumer = KafkaConsumer('messages',
                                 group_id='my_group',
                                 bootstrap_servers=['localhost:9092'])


    print ("Consumed Start") 
    for message in consumer:
        yield template("Hello {{string}} <br/>", string=message.value.decode('utf-8'))   
        print ("Consumed Msg -> '%s' on Topic -> '%s' with Offset -> %d" %
                  (message.value.decode('utf-8'), message.topic, message.offset))
    

    
   
    print ("Consumer Close") 
    consumer.close()

    

if __name__ == "__main__":
    run(host="localhost", port=8080, debug=True, reloader=True)

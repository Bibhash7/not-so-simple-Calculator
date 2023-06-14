from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
mydatabase = client.mydb
mycollection = mydatabase.calculator


topic_name='numtest'
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='group',
        value_deserializer=lambda x: loads(x.decode('utf-8')))

while(1):

    data = input("Enter an arithmetic expression: (Press [x] to exit: ")
    producer.send(topic_name, value=data)
    consumer.commit()

    if (data == "x"):
        print("Thanks for using not-so-Simple-Calculator:")
        break

    for expression in consumer:
        topic_read = expression.value.strip()
        topic_read = topic_read.replace(" ","")
        operators = "+-*/%"
        topic_length = len(topic_read)
        operator = ""
        for char in topic_read:
            if char.isnumeric():
                topic_length-=1
            else:
                operator = char
        if ((topic_length == 1) and (operator in operators)):
            value_string = topic_read.split(operator)
            firstnum = int(value_string[0])
            secondnum = int(value_string[1])
            result = 0
            if(operator == '+'):
                result = firstnum+secondnum
            if (operator == '-'):
                result = firstnum-secondnum
            if (operator == '*'):
                result = firstnum*secondnum
            if (operator == '/'):
                result = firstnum/secondnum
            if (operator == '%'):
                result = firstnum%secondnum
            print("Response added to the database: The result for your expression: ({})={}".format(topic_read,result))
            document = {
                "First Number": firstnum,
                "Operator": operator,
                "Second Number": secondnum,
                "Result": result
            }
            rec = mycollection.insert_one(document)
        else:
            print("Invalid Expression: Please enter a valid expression:")
        break



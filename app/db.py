import pymysql

def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="YOUR_PASSWORD",
        database="flight_weather",
        cursorclass=pymysql.cursors.DictCursor
    )
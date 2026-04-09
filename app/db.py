import pymysql


def get_connection():
    return pymysql.connect(
        host="localhost",
        user="ia626app",
        password="ia626app123",
        database="flight_weather",
        cursorclass=pymysql.cursors.DictCursor,
    )
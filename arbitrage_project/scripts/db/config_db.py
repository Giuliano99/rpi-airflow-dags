import psycopg2

def get_connection():
    return psycopg2.connect(
        dbname="arbitrage_project",
        user="postgres",
        password="5ads15",
        host="172.17.0.2",
        port="5432"
    )

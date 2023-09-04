import psycopg2
import pandas as pd

db_name = "dvd_rental"
db_user = "postgres"
db_pass = "admin"
db_host = "localhost"
db_port = 5432

# Creating psycopg2 connection to PostgreSQL
def create_connection():
    db_session = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_pass,
        host = db_host,
        port = db_port
    )
    return db_session

# Closing / Disposing the db_session connection
def close_connection(db_session):
    db_session.close()


# GET QUERY AS DF
def return_query_as_df(db_session,query):
    query_df = pd.read_sql_query(sql= query, con=db_session)
    return query_df

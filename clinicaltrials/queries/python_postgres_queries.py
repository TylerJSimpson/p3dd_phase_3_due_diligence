import psycopg2 as pg
import pandas.io.sql as psql
connection = pg.connect("host=aact-db.ctti-clinicaltrials.org dbname=aact user=tjsimpson password=672Delta")
dataframe = psql.read_sql('SELECT * FROM studies LIMIT 10', connection)
#product_category = psql.read_sql_query('select * from product_category', connection)

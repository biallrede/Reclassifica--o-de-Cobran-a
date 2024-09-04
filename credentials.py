import psycopg2
import pyodbc

def credenciais_banco():
    conn = psycopg2.connect(
                        host='134.65.24.116',
                        port='9432',
                        database='hubsoft',
                        user='erick_leitura',
                        password='73f4cc9b2667d6c44d20d1a0d612b26c5e1763c2')
    
    return conn

def credenciais_banco_teste():
    conn = psycopg2.connect(
                        host ='177.66.167.20',
                        port = '9432',
                        database='hubsoft',
                        user='novos_produtos',
                        password='Hak68897efg567'
                        )
    
    return conn



def credenciais_banco_token():
# Configuração da conexão com o banco de dados
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'Server=187.121.151.19;' 
        'Database=DB_BASE;'
        'UID=user_allnexus;'
        'PWD=uKl041xn8HIw0WF;'
        'TrustServerCertificate=yes;'
    )
    return conn

# def credenciais_banco_token():
# # Configuração da conexão com o banco de dados
#     conn = pyodbc.connect(
#         'DRIVER={ODBC Driver 17 for SQL Server};'
#         'Server=187.121.151.19;' 
#         'Database=DB_BASE;'
#         'UID=user_allnexus;'
#         'PWD=uKl041xn8HIw0WF;'
#     )
#     return conn
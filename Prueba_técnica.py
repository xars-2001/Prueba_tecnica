import pandas as pd
import datetime
import mysql.connector
from sqlalchemy import DECIMAL, TIMESTAMP, Column, MetaData, String, Table, create_engine

#obteniendo los datos del archivo csv
df=pd.read_csv("data_prueba_tecnica.csv")

#obteniendo los datos por columna
id_hex=df["id"]
company_name=df["name"]
company_id=df["company_id"]
amount=df["amount"]
status=df["status"]
created_at=df["created_at"]
paid_at=df["paid_at"]

#construyendo la nueva base de datos
df1=pd.DataFrame(data=[company_name,amount,status,created_at,paid_at])
df1=df1.transpose()

# Renombrar la columna "name" a "company_name"
df1 = df1.rename(columns={'name': 'company_name'})

#agregando un id único
df1['id']=df1.index
first_col=df1.pop('id')
df1.insert(0,'id',first_col)

#creando tabla que relaciona id con id_hex
df_id=pd.DataFrame(data=[first_col,id_hex,company_id])
df_id=df_id.transpose()

#Cambiando '8f642dc67fccf861548dfe1c761ce22f795e91f0' por 'A'
#Cambiando 'cbf1c8b09cd5b549416d49d220a40cbd317f952e' por 'B'
df_id['company_id']=df_id['company_id'].replace('*******', 'cbf1c8b09cd5b549416d49d220a40cbd317f952e')
df_id['company_id']=df_id['company_id'].replace('', 'cbf1c8b09cd5b549416d49d220a40cbd317f952e')
df_id['comp_id'] = df_id['company_id'].replace('8f642dc67fccf861548dfe1c761ce22f795e91f0', 'A')
df_id['comp_id'] = df_id['company_id'].replace('cbf1c8b09cd5b549416d49d220a40cbd317f952e', 'B')

#Pasando la información al dataframe principal
df1['company_id']=df_id['comp_id']
third_col=df1.pop('company_id')
df1.insert(2,'company_id',third_col)

#reemplazar datos que no habían sido cambiados
df1['company_id'] = df1['company_id'].replace('8f642dc67fccf861548dfe1c761ce22f795e91f0', 'A')
df1.at[262,'company_id']="B"
df1.at[2378,'company_id']="B"
df1.at[2445,'company_id']="B"
df1.at[5981,'company_id']="B"

#Analizando los requisitos de nuevo
"""#los requisitos que deben cumplir los campos son:
id varchar(24) NOT NULL
company_name varchar(130) NULL
company_id varchar(24) NOT NULL
amount decimal(16,2) NOT NULL
status marchar(30) NOT NULL
created_at timestamp NOT NULL
updated_at timestamp NULL"""

#Cambiando el tipo de dato de cada columna
df1['id']=df1['id'].astype(str)
df1['amount']=df1['amount'].astype(float).round(2)

#El campo created_at no está en el formato correcto así que hay que modificar ciertos registros
#Modificaremos los registros 732,734 y 830
df1.at[732,'created_at']="2019-02-27"
df1.at[734,'created_at']="2019-05-16"
df1.at[830,'created_at']="2019-01-21"

#Cambiando el tipo de dato de las fechas
df1['created_at']=pd.to_datetime(df1['created_at'],format='%Y-%m-%d')
df1['paid_at']=pd.to_datetime(df1['paid_at'],format='%Y-%m-%d')

#Modificaremos los registros 732,734 y 830
df1.at[1515,'amount']="9999999"
df1.at[1609,'amount']="9999999"
df1.at[1748,'amount']="9999999"
df1.at[1896,'amount']="9999999"

#filtrando datos de más de 10 dígitos
df_filtered = df1[df1['amount'].astype(str).str.len() >= 10]

#print(df_filtered)
df1.sort_values(by='id')

#Definiendo parámetros de conexión
db_df1 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="Prueba_tecnica"
)
mycursor=db_df1.cursor() #Definiendo un cursor
query='DROP TABLE prueba_tec' #Creando el comando
mycursor.execute(query)  #Ejecutando el comando

#conectando con MySQL Server
engine = create_engine("mysql+pymysql://root:admin@localhost/Prueba_tecnica")

# Crear una instancia de MetaData
metadata = MetaData()

# Definir la tabla con formato y valores
prueba_tec = Table('prueba_tec', metadata,
                   Column('id', String(24), primary_key=True),
                   Column('company_name', String(130), nullable=True),
                   Column('company_id', String(24), nullable=False),
                   Column('amount', DECIMAL(16, 2), nullable=False),
                   Column('status', String(30), nullable=False),
                   Column('created_at', TIMESTAMP, nullable=False),
                   Column('paid_at', TIMESTAMP, nullable=True))

# Crear la tabla en la base de datos
metadata.create_all(engine)
df1.to_sql('prueba_tec', con=engine, if_exists='append', index=False)

# Eliminar las tablas si ya existen
mycursor.execute('DROP TABLE IF EXISTS charges')
mycursor.execute('DROP TABLE IF EXISTS companies')

# Crear la tabla companies con un id nuevo como clave primaria
mycursor.execute('''
    CREATE TABLE companies (
        company_id INT AUTO_INCREMENT PRIMARY KEY,
        company_code VARCHAR(1) UNIQUE,
        company_name VARCHAR(100) NOT NULL,
        location VARCHAR(100),
        contact_person VARCHAR(100)
    )
''')

# Crear la tabla charges con un id nuevo como clave primaria
mycursor.execute('''
    CREATE TABLE charges (
        charge_id INT AUTO_INCREMENT PRIMARY KEY,
        amount DECIMAL(10, 2) NOT NULL,
        transaction_date DATE NOT NULL,
        company_id INT,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
    )
''')

# Datos faltantes del dataset
companies_data = [
    ('A', 'MueblesChidos', 'Aguascalientes, México', 'Andrea Tamayo'),
    ('B', 'MiPasajefy', 'Puebla, México', 'Adolfo Galicia')
]

# Insertar datos en la tabla companies
insert_company_query = '''
    INSERT INTO companies (company_code, company_name, location, contact_person)
    VALUES (%s, %s, %s, %s)
'''
mycursor.executemany(insert_company_query, companies_data)
db_df1.commit()

# Obtener el mapping de company_code a company_id
mycursor.execute('SELECT company_id, company_code FROM companies')
company_mapping = {code: id for id, code in mycursor.fetchall()}

# Convertir los datos de df1 a usar company_id y transformarlos en una lista de tuplas
charges_data_mapped = [
    (row['amount'], row['created_at'], company_mapping[row['company_id']])
    for index, row in df1.iterrows()
]

# Insertar datos en la tabla charges
insert_charges_query = '''
    INSERT INTO charges (amount, transaction_date, company_id)
    VALUES (%s, %s, %s)
'''
mycursor.executemany(insert_charges_query, charges_data_mapped)
db_df1.commit()

mycursor.execute('''CREATE OR REPLACE VIEW daily_transaction_totals AS
    SELECT
        comp.company_name,
        DATE(c.transaction_date) AS transaction_day,
        SUM(c.amount) AS total_amount
    FROM
        charges c
        JOIN companies comp ON c.company_id = comp.company_id
    GROUP BY
        comp.company_name, transaction_day;''')

# Consultar la vista daily_transaction_totals
mycursor.execute("SELECT * FROM daily_transaction_totals ORDER BY transaction_day")

# Obtener todos los resultados
results = mycursor.fetchall()

# Mostrar los resultados
for row in results:
    print(row)

# Cerrar el cursor y la conexión a la base de datos
mycursor.close()
db_df1.close()
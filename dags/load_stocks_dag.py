from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import logging
from configparser import ConfigParser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from sqlalchemy import create_engine
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def load_stocks():
    """
    Inicializa todas las demas funciones.
    """
    config = ConfigParser()
    config.read("config/config.ini")
    key = config.get('api', 'key')
    base_url="http://api.marketstack.com/v1/tickers"
    endpoint="YPF/eod/latest"
    df = get_data(base_url, endpoint, key)
    engine = connect_to_db("config/config.ini","redshift")
    load_to_sql(df, engine)
    send_email("config/config.ini", df)


def send_email(config_file, df):
    """
    Funcion para mandar un email.
    """
    config = ConfigParser()
    config.read(config_file)
    smtp_server = config.get('SMTP', 'server')
    smtp_port = config.getint('SMTP', 'port')
    smtp_username = config.get('SMTP', 'username')
    smtp_password = config.get('SMTP', 'password')

    sender_email = config.get('Email', 'sender')
    receiver_email = config.get('Email', 'receiver')
    subject = "Pipeline completado"
    message = "Resumen de acciones" #Agregar df

    # Crear el objeto del mensaje
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    # Agregar el cuerpo del mensaje
    msg.attach(MIMEText(message, 'plain'))

    # Iniciar sesión en el servidor SMTP
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())


def get_data(base_url, endpoint, key):
    """
    Realiza una solicitud GET a una API para devolver los datos
    en formato json y transformados.

    @base_url: URL base de la API.
    @endpoint: Endpoint de la API para especificar datos.
    @params: Parámetros de la solicitud que incluyen la secret key.
    """
    params = {
    'access_key': key
    }

    try:
        endpoint_url = f"{base_url}/{endpoint}"
        logging.info(f"Obteniendo datos de {endpoint_url}...")
        logging.info(f"Parámetros: {params}")
        response = requests.get(endpoint_url, params=params)
        response.raise_for_status()
        logging.info(response.url)
        logging.info("Datos obtenidos exitosamente... Procesando datos...")
        data = response.json()

        logging.info(f"Datos procesados exitosamente")
    except Exception as e:
        logging.exception(f"Error al obtener datos de {base_url}: {e}")

    data = {
    'apertura': data['open'],
    'precio_alto': data['high'],
    'precio_bajo': data['low'],
    'cierre': data['close'],
    'volumen': data['volume'],
    'simbolo': data['symbol'],
    'fecha': data["date"]
    }
    df = pd.DataFrame([data])
    return df
    
def connect_to_db(config_file, section):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    @config_file: La ruta del archivo de configuración.
    @section: La sección del archivo de configuración que contiene los datos de la base de datos.
    """
    try:
        parser = ConfigParser()
        parser.read(config_file)

        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            logging.info("Conectándose a la base de datos...")
            engine = create_engine(
                f"postgresql://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}"
                , connect_args={"options": f"-c search_path={db['schema']}"}
                )

            logging.info("Conexión a la base de datos establecida exitosamente")
            return engine

        else:
            logging.error(f"Sección {section} no encontrada en el archivo de configuración")
            return None
        
    except Exception as e:
        logging.error(f"Error al conectarse a la base de datos: {e}")
        return None
    
def load_to_sql(df, engine):
    """
    Cargar un dataframe en una tabla de base de datos,
    usando una tabla intermedia o stage para control de duplicados.

    @Dataframe
    @Motor para la conexion de la db.
    """
    conn = engine.connect()
    df.to_sql(name="api",
    con=conn,
    schema="palombopabloe_coderhouse",
    if_exists="append",
    method="multi",
    index=False,
    )



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_stocks_hourly',
    default_args=default_args,
    description='Cargar datos de stocks cada hora',
    schedule_interval='@hourly',
)


load_stocks_task = PythonOperator(
    task_id='load_stocks_task',
    python_callable=load_stocks,
    dag=dag,
)

load_stocks_task

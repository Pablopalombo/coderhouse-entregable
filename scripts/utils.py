import logging
from configparser import ConfigParser

import requests
from sqlalchemy import create_engine
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        # Registrar cualquier otro error
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
    @Motor para hacer la conexion a la db.
    """
    conn = engine.connect()
    df.to_sql(name="api",
    con=conn,
    schema="palombopabloe_coderhouse",
    if_exists="append",
    method="multi",
    index=False,
    )

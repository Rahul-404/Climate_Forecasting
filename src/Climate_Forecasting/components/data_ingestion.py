from src.Climate_Forecasting.logger import logging
from src.Climate_Forecasting.exception import customexception
from sklearn.model_selection import train_test_split
from dataclasses import dataclass
import mysql.connector
from mysql.connector import Error
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import os
import sys

# calling all env variable
load_dotenv()

class DataIngestionConfig:
    raw_data_path:str = os.path.join("artifacts", "raw.csv")
    train_data_path:str = os.path.join("artifacts", "train.csv")
    test_data_path:str = os.path.join("artifacts", "test.csv")
    host:str = os.getenv('HOST')
    user:str = os.getenv('USER')
    password:str = os.getenv('PASSWORD')
    database:str = os.getenv('DB')


class DataIngestion:

    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def create_connection(self):
        logging.info("[-] Establishing connection with databse...")

        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.ingestion_config.host,
                user=self.ingestion_config.user,
                password=self.ingestion_config.password,
                database=self.ingestion_config.database
            )
            if connection.is_connected():
                print("Connection to MySQL DB successful")
                logging.info("[*] Established connection successfully!")

            return connection
        
        except Error as e:
            print(f"[!] Exception occured during creating connection with databse stage: {e}")
            raise customexception(e, sys)

    def initiate_data_ingestion(self):
        logging.info("[-] Data Ingestion Started...")

        try:
            # connect with sql
            connection = self.create_connection()

            if connection is None:
                data = pd.read_csv(Path(os.path.join("notebooks/data", "raw_data.csv")))

            else:
                pass
                # query to get data

                # convert it to csv

                # read data from SQL

            logging.info("[*] Data Ingestion Completed!")

            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok=True)
            data.to_csv(self.ingestion_config.raw_data_path, index=False)


            logging.info("[-] Performing Train-Test Split...")
            total_points = int(data.shape[0]*0.8)

            train, test = data.iloc[:total_points,:], data.iloc[total_points:,:]

            train.to_csv(self.ingestion_config.train_data_path, index=False)
            test.to_csv(self.ingestion_config.test_data_path, index=False)

            logging.info("[*] Train-Test Split Completed!")


        except Exception as e:
            logging.info(f"[!] Exception occured during Data Ingestion stage : {e}")
            raise customexception(e, sys)
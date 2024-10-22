from src.Climate_Forecasting.logger import logging
from src.Climate_Forecasting.exception import customexception

from sklearn.model_selection import train_test_split
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import os
import sys

class DataIngestionConfig:
    raw_data_path:str = os.path.join("artifacts", "raw.csv")
    train_data_path:str = os.path.join("artifacts", "train.csv")
    test_data_path:str = os.path.join("artifacts", "test.csv")


class DataIngestion:

    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        logging.info("[-] Data Ingestion Started...")

        try:
            # read data from SQL
            data = pd.read_csv(Path(os.path.join("notebooks/data", "raw_data.csv")))
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
            logging.info(f"[!] Exception occured during Data Ingestion stage")
            raise customexception(e, sys)
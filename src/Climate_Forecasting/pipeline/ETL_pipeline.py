from src.Climate_Forecasting.components.data_ingestion import DataIngestion
from src.Climate_Forecasting.exception import customexception
from src.Climate_Forecasting.logger import logging
import os
import sys 


obj = DataIngestion()

# obj.initiate_data_ingestion()

obj.create_connection()

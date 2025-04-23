from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

# Configuration files usually contain details like file paths, database names, etc.
from networksecurity.entity.config_entity import DataIngestionConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact
import os
import sys
import numpy as np
import pymongo
import pandas as pd
from sklearn.model_selection import train_test_split
from typing import List

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")


class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_collection_as_dataframe(self):
        """Reads data from MongoDB and converts it into a DataFrame."""
        try:
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]
            df = pd.DataFrame(list(collection.find()))

            # Drop _id column if it exists in the DataFrame
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)

            # Replace "na" with NaN for consistency
            df.replace({"na": np.nan}, inplace=True)

            logging.info("Successfully exported collection from MongoDB to DataFrame.")
            return df

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_data_into_feature_store(self, dataframe: pd.DataFrame):
        """Exports the DataFrame to a CSV file, which acts as the feature store."""
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)

            # Save the DataFrame to CSV
            dataframe.to_csv(feature_store_file_path, index=False, header=True)

            logging.info(f"Data exported to feature store at {feature_store_file_path}.")
            return dataframe

        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        """Splits the DataFrame into training and testing datasets."""
        try:
            # Ensure the splitting ratio is valid
            test_size = self.data_ingestion_config.train_test_split_ratio
            if not (0 < test_size < 1):
                raise ValueError(f"Invalid train-test split ratio: {test_size}. It should be between 0 and 1.")

            # Split the data into train and test sets
            train_set, test_set = train_test_split(dataframe, test_size=test_size)

            logging.info("Performed train-test split on the DataFrame.")

            # Create directories for train and test file exports
            train_dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            test_dir_path = os.path.dirname(self.data_ingestion_config.testing_file_path)

            os.makedirs(train_dir_path, exist_ok=True)
            os.makedirs(test_dir_path, exist_ok=True)

            logging.info("Exporting train and test datasets to specified file paths.")

            # Save the train and test datasets to CSV
            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)

            logging.info("Train and test datasets exported successfully.")
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_ingestion(self):
        """Initiates the entire data ingestion pipeline."""
        try:
            # Step 1: Load data from MongoDB
            dataframe = self.export_collection_as_dataframe()

            # Step 2: Export data to feature store
            dataframe = self.export_data_into_feature_store(dataframe)

            # Step 3: Split data into train and test
            self.split_data_as_train_test(dataframe)

            # Step 4: Return DataIngestionArtifact with file paths
            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )

            logging.info("Data ingestion completed successfully.")
            return data_ingestion_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)

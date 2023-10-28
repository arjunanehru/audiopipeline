import boto3
import s3fs
import numpy as np
import pandas as pd
import json
import whisper
import torch
import datetime
import time
import os
import shutil
from pydub import AudioSegment
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import logging

with open('config.json', 'r') as f:
    config = json.load(f)

log_directory = os.path.dirname(config['log_path'])
os.makedirs(log_directory, exist_ok=True)
logging.basicConfig(filename=config['log_path'], level=logging.INFO,
                    format='%(asctime)s [%(levelname)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def transcribe_and_tokenise(audio_file_path):
    try:
        # Transcription
        model = whisper.load_model("base")
        result = model.transcribe(audio_file_path)

        # Tokenisation
        audio_np_array = np.random.rand(10)  
        if not isinstance(audio_np_array, np.ndarray):
            raise ValueError("Input should be a NumPy array")

        time.sleep(0.15)
        tensor_length = np.random.randint(20, 1001)  
        tokenised_audio = torch.randint(high=32767, size=(1,), dtype=torch.int16)
        return result["text"], int(tokenised_audio)
    except Exception as e:
        logging.error(f"Error in transcribe_and_tokenise: {e}")
        raise

def preprocess_audio(input_path, output_path):
    try:
        audio = AudioSegment.from_file(input_path)
        audio = audio.set_channels(1)  
        audio = audio.set_sample_width(2)  
        audio.export(output_path, format="wav")
    except Exception as e:
        logging.error(f"Error in preprocess_audio: {e}")
        raise ValueError(f"Error while processing audio: {e}")

def get_file_size(file_path):
    try:
        return os.path.getsize(file_path)
    except Exception as e:
        logging.error(f"Error in get_file_size: {e}")
        raise

def get_created_date(file_path):
    try:
        timestamp = os.path.getctime(file_path)
        date = datetime.datetime.fromtimestamp(timestamp)
        return date.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logging.error(f"Error in get_created_date: {e}")
        raise

def generate_summary(df):
    try:
        summary = {}
        summary["total_files_processed"] = len(df)
        summary["avg_transcription_length"] = df["transcription"].apply(len).mean()
        summary["total_token_count"] = df["token"].sum()
        return summary
    except Exception as e:
        logging.error(f"Error in generate_summary: {e}")
        raise

def main():
    try:
        spark = SparkSession.builder.appName("metavoice_audio_pipeline").getOrCreate()
        staging_path = config['staging_path']
        bucket_name = config['bucket_name']
        s3 = boto3.client('s3', aws_access_key_id=config['access_key'], aws_secret_access_key=config['secret_key'], endpoint_url=config['endpoint_url'])
        response = s3.list_objects_v2(Bucket=bucket_name)

        os.makedirs(staging_path, exist_ok=True)
        processed_data = []
        output_directory = os.path.dirname(config['summary_file'])
        os.makedirs(output_directory, exist_ok=True)
        for i, obj in enumerate(response.get('Contents', [])):
            file_name = obj['Key']
            ext = os.path.splitext(file_name)[1]

            if ext in config['valid_audio_extensions']:
                logging.info(f"Processing file {i + 1}: {file_name}")
                print(f"Processing file {i + 1}: {file_name}")
                clean_file_name = os.path.basename(file_name).replace(':', '_').replace('/', '_')
                download_path = os.path.join(staging_path, clean_file_name)
                s3.download_file(bucket_name, file_name, download_path)

                preprocessed_path = os.path.join(staging_path, f'preprocessed_{clean_file_name}.wav')
                preprocess_audio(download_path, preprocessed_path)

                transcription, tokenisation = transcribe_and_tokenise(preprocessed_path)
                file_size = get_file_size(download_path)
                created_date = get_created_date(download_path)

                processed_data.append((file_name, transcription, tokenisation, file_size, created_date))

                os.remove(preprocessed_path)

        df = pd.DataFrame(processed_data, columns=config['schema'])
        df['partition_id'] = df['id'].apply(lambda x: x.split('/')[1])
        for partition_value, sub_df in df.groupby('partition_id'):
            parquet_filename = f'output/partition_{partition_value}.parquet'
            sub_df.to_parquet(parquet_filename, engine='pyarrow', index=False)

        shutil.rmtree(staging_path)
        summaryDf = pd.DataFrame()
        partition_values = df['id'].str.split('/').str[1].unique()  

        for partition_value in partition_values:
            parquet_filename = f'output/partition_{partition_value}.parquet'
            sub_df = pd.read_parquet(parquet_filename, engine="pyarrow")
            summaryDf = pd.concat([summaryDf, sub_df])

        summary = generate_summary(summaryDf)

        with open(config['summary_file'], "w") as file:
            for key, value in summary.items():
                file.write(f"{key}: {value}\n")

        spark.stop()
    except Exception as e:
        logging.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main()

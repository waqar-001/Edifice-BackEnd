from flask import Flask, jsonify
import boto3
from dotenv import load_dotenv
import os
import pandas as pd
from io import BytesIO
from mlxtend.frequent_patterns import apriori
import threading
import time

# Initialize Flask app
app = Flask(__name__)

# Load environment variables from the .env file
load_dotenv()

# Retrieve AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('REGION')
bucket_name = os.getenv('BUCKET_NAME')

# Initialize the S3 client
s3 = boto3.client(
    's3',
    region_name=region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Define the local directory to save the downloaded Parquet files
local_directory = 'downloaded_parquet_files'

# Create the directory if it doesn't exist
if not os.path.exists(local_directory):
    os.makedirs(local_directory)

def fetch_files():
    all_frequent_itemsets = []
    response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    directories = [content['Prefix'] for content in response.get('CommonPrefixes', [])]

    if not directories:
        return jsonify({'error': 'No directories found'}), 404

    for directory in directories:
        # List all Parquet files in the current directory
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory)
        files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.parquet')]

        for i in range(0, len(files), 10):  # Fetch 10 files at a time
            batch_files = files[i:i + 10]

            for file_key in batch_files:
                # Download the Parquet file from S3
                response = s3.get_object(Bucket=bucket_name, Key=file_key)
                parquet_data = response['Body'].read()

                # Save the Parquet file to the local directory
                local_file_path = os.path.join(local_directory, file_key.replace('/', '_'))
                with open(local_file_path, 'wb') as f:
                    f.write(parquet_data)

                # Load the Parquet data into a DataFrame
                parquet_io = BytesIO(parquet_data)
                df = pd.read_parquet(parquet_io)

                # Perform data processing and Apriori as before
                non_binary_columns = ['timestamp', 'some_other_column']  # Adjust column names as necessary
                df = df.drop(columns=non_binary_columns, errors='ignore')
                df = df.astype(bool)

                frequent_itemsets = apriori(df, min_support=0.6, use_colnames=True)
                frequent_itemsets['itemsets'] = frequent_itemsets['itemsets'].apply(lambda x: list(x))
                frequent_itemsets_dict = frequent_itemsets.to_dict(orient='records')
                all_frequent_itemsets.extend(frequent_itemsets_dict)

            time.sleep(300)  # Wait for 5 seconds after fetching each batch

    return jsonify(all_frequent_itemsets)

@app.route('/start_fetching', methods=['GET'])
def start_fetching():
    thread = threading.Thread(target=fetch_files)
    thread.start()
    return jsonify({'message': 'Fetching started in the background'}), 202

if __name__ == '__main__':
    app.run(debug=True)

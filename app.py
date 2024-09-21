# from flask import Flask, jsonify
# import boto3
# from dotenv import load_dotenv
# import os
# import pandas as pd
# import pyarrow.parquet as pq
# from io import BytesIO
# from mlxtend.frequent_patterns import apriori

# # Initialize Flask app
# app = Flask(__name__)

# # Load environment variables from the .env file
# load_dotenv()

# # Retrieve AWS credentials from environment variables
# aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
# aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
# region = os.getenv('REGION')
# bucket_name = os.getenv('BUCKET_NAME')

# # Initialize the S3 client
# s3 = boto3.client(
#     's3',
#     region_name=region,
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key
# )

# # Specify the top-level directory in the S3 bucket
# top_level_prefix = ''

# @app.route('/fetch_data', methods=['GET'])
# def fetch_data():
#     try:
#         # List all directories (folders) at the top level
#         response = s3.list_objects_v2(Bucket=bucket_name, Prefix=top_level_prefix, Delimiter='/')
#         directories = [content['Prefix'] for content in response.get('CommonPrefixes', [])]

#         if not directories:
#             return jsonify({'error': 'No directories found'}), 404

#         all_frequent_itemsets = []

#         for directory in directories:
#             # List all Parquet files in the current directory
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory)
#             files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.parquet')]

#             for file_key in files:
#                 # Download the Parquet file from S3
#                 response = s3.get_object(Bucket=bucket_name, Key=file_key)
#                 parquet_data = response['Body'].read()

#                 # Load the Parquet data into a DataFrame
#                 parquet_io = BytesIO(parquet_data)
#                 df = pd.read_parquet(parquet_io)

#                 # Drop non-binary columns such as dates or continuous data
#                 non_binary_columns = ['timestamp', 'some_other_column']  # Adjust column names as necessary
#                 df = df.drop(columns=non_binary_columns, errors='ignore')

#                 # Convert all remaining columns to boolean (True/False)
#                 df = df.astype(bool)

#                 # Show frequent itemsets with a minimum support of 0.6
#                 frequent_itemsets = apriori(df, min_support=0.6, use_colnames=True)

#                 # Convert frozensets to lists
#                 frequent_itemsets['itemsets'] = frequent_itemsets['itemsets'].apply(lambda x: list(x))

#                 # Convert the frequent itemsets DataFrame to a dictionary
#                 frequent_itemsets_dict = frequent_itemsets.to_dict(orient='records')
#                 all_frequent_itemsets.extend(frequent_itemsets_dict)

#         # Return the combined JSON response
#         return jsonify(all_frequent_itemsets)

#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True)

# from flask import Flask, jsonify
# import boto3
# from dotenv import load_dotenv
# import os
# import pandas as pd
# import pyarrow.parquet as pq
# from io import BytesIO
# from mlxtend.frequent_patterns import apriori

# # Initialize Flask app
# app = Flask(__name__)

# # Load environment variables from the .env file
# load_dotenv()

# # Retrieve AWS credentials from environment variables
# aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
# aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
# region = os.getenv('REGION')
# bucket_name = os.getenv('BUCKET_NAME')

# # Initialize the S3 client
# s3 = boto3.client(
#     's3',
#     region_name=region,
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key
# )

# @app.route('/fetch_data', methods=['GET'])
# def fetch_data():
#     try:
#         all_frequent_itemsets = []

#         # List all top-level folders
#         response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
#         print("All the Response ", response)
#         directories = [content['Prefix'] for content in response.get('CommonPrefixes', [])]
        
#         print("All the Directories  ", directories)

#         if not directories:
#             return jsonify({'error': 'No directories found'}), 404

#         for directory in directories:
#             # List all Parquet files in the current directory
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory)
#             files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.parquet')]

#             for file_key in files:
#                 # Download the Parquet file from S3
#                 response = s3.get_object(Bucket=bucket_name, Key=file_key)
#                 parquet_data = response['Body'].read()

#                 # Load the Parquet data into a DataFrame
#                 parquet_io = BytesIO(parquet_data)
#                 df = pd.read_parquet(parquet_io)

#                 # Drop non-binary columns such as dates or continuous data
#                 non_binary_columns = ['timestamp', 'some_other_column']  # Adjust column names as necessary
#                 df = df.drop(columns=non_binary_columns, errors='ignore')

#                 # Convert all remaining columns to boolean (True/False)
#                 df = df.astype(bool)

#                 # Show frequent itemsets with a minimum support of 0.6
#                 frequent_itemsets = apriori(df, min_support=0.6, use_colnames=True)

#                 # Convert frozensets to lists
#                 frequent_itemsets['itemsets'] = frequent_itemsets['itemsets'].apply(lambda x: list(x))

#                 # Convert the frequent itemsets DataFrame to a dictionary
#                 frequent_itemsets_dict = frequent_itemsets.to_dict(orient='records')
#                 all_frequent_itemsets.extend(frequent_itemsets_dict)

#         # Return the combined JSON response
#         return jsonify(all_frequent_itemsets)

#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True)


# this will best one parquest 
# from flask import Flask, jsonify
# import boto3
# from dotenv import load_dotenv
# import os
# import pandas as pd
# from io import BytesIO

# # Initialize Flask app
# app = Flask(__name__)

# # Load environment variables from the .env file
# load_dotenv()

# # Retrieve AWS credentials from environment variables
# aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
# aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
# region = os.getenv('REGION')
# bucket_name = os.getenv('BUCKET_NAME')

# # Initialize the S3 client
# s3 = boto3.client(
#     's3',
#     region_name=region,
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key
# )

# # Define the local directory to save the downloaded Parquet files
# local_directory = 'downloaded_parquet_files'

# # Create the directory if it doesn't exist
# if not os.path.exists(local_directory):
#     os.makedirs(local_directory)

# @app.route('/fetch_data', methods=['GET'])
# def fetch_data():
#     try:
#         all_frequent_itemsets = []

#         # List all top-level folders
#         response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
#         directories = [content['Prefix'] for content in response.get('CommonPrefixes', [])]

#         if not directories:
#             return jsonify({'error': 'No directories found'}), 404

#         for directory in directories:
#             # List all Parquet files in the current directory
#             response = s3.list_objects_v2(Bucket=bucket_name, Prefix=directory)
#             files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.parquet')]

#             for file_key in files:
#                 # Download the Parquet file from S3
#                 response = s3.get_object(Bucket=bucket_name, Key=file_key)
#                 parquet_data = response['Body'].read()

#                 # Save the Parquet file to the local directory
#                 local_file_path = os.path.join(local_directory, file_key.replace('/', '_'))
#                 with open(local_file_path, 'wb') as f:
#                     f.write(parquet_data)

#                 # Optionally load the Parquet data into a DataFrame
#                 parquet_io = BytesIO(parquet_data)
#                 df = pd.read_parquet(parquet_io)

#                 # Perform data processing and Apriori as before, if needed
#                 non_binary_columns = ['timestamp', 'some_other_column']  # Adjust column names as necessary
#                 df = df.drop(columns=non_binary_columns, errors='ignore')
#                 df = df.astype(bool)

#                 frequent_itemsets = apriori(df, min_support=0.6, use_colnames=True)
#                 frequent_itemsets['itemsets'] = frequent_itemsets['itemsets'].apply(lambda x: list(x))
#                 frequent_itemsets_dict = frequent_itemsets.to_dict(orient='records')
#                 all_frequent_itemsets.extend(frequent_itemsets_dict)

#         # Return the combined JSON response of frequent itemsets
#         return jsonify(all_frequent_itemsets)

#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

# if __name__ == '__main__':
#     app.run(debug=True)





import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
import os

app = Flask(__name__)

# Folder where the uploaded files are stored
UPLOAD_FOLDER = 'uploaded_files'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Route to process the selected sheet from an uploaded Excel file
@app.route('/process', methods=['POST'])
def process_data():
    try:
        # Get the file name and sheet name from the JSON request body
        data = request.get_json()
        file_name = data.get('file_name')
        sheet_name = data.get('sheet_name')

        # Check if the file exists in the specified folder
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
        if not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404

        # Load the specified sheet from the Excel file
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)

        # Remove 'Unnamed' columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Convert the 'datetime' column to actual datetime objects and extract the weekday
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df['weekday'] = df['datetime'].dt.day_name()

        # Map full weekday names to short forms
        weekday_map = {
            'Monday': 'Mon', 'Tuesday': 'Tue', 'Wednesday': 'Wed', 'Thursday': 'Thu',
            'Friday': 'Fri', 'Saturday': 'Sat', 'Sunday': 'Sun'
        }
        df['weekday_short'] = df['weekday'].map(weekday_map)

        # Aggregate data by weekday
        weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        agg_data = df.groupby('weekday_short').agg(
            conversion_desk_total=('conversion_front_desk', 'sum'),
            conversion_self_checkout_total=('conversion_self_checkout', 'sum'),
            bounce_total=('bounce', 'sum'),
            turnout_total=('marketplace_turnout', 'sum')
        ).reindex(weekdays)

        # Calculate percentages and averages
        agg_data['conversion_desk_%'] = (agg_data['conversion_desk_total'] / agg_data['turnout_total'] * 100).fillna(0).round(2)
        agg_data['conversion_self_checkout_%'] = (agg_data['conversion_self_checkout_total'] / agg_data['turnout_total'] * 100).fillna(0).round(2)
        agg_data['bounce_%'] = (agg_data['bounce_total'] / agg_data['turnout_total'] * 100).fillna(0).round(2)
        agg_data['turnout_avg'] = (agg_data['turnout_total'] / len(df['datetime'].unique())).round(2)

        # Add placeholder sales per guest and average conversion columns
        agg_data['sales_per_guest'] = np.nan  # Placeholder
        agg_data['conversion_avg'] = agg_data['conversion_desk_%'].mean().round(2)

        # Reset the index for display purposes
        agg_data.reset_index(inplace=True)
        agg_data.rename(columns={'weekday_short': 'Weekday'}, inplace=True)

        # Return the aggregated data as JSON
        return jsonify(agg_data.to_dict(orient='records'))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)

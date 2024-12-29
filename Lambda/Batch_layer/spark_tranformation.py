import json
from hdfs import InsecureClient
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, FloatType
from transformers import BertTokenizer, BertModel
import torch

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Text Data Transformation for Chatbot") \
    .getOrCreate()

# Load the BERT model and tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')

def return_spark_df():
    """
    Read JSON data from HDFS and convert to a Spark DataFrame.
    """
    hdfs_client = InsecureClient('http://localhost:9870')

    with hdfs_client.read("/batch-layer/website_data.json") as reader:
        pandas_df = pd.read_json(reader, lines=True)  # Assuming JSON lines format

    spark_df = spark.createDataFrame(pandas_df)

    return spark_df

def transform_text(text):
    """
    Transform the raw text into BERT embeddings.
    """
    inputs = tokenizer(text, return_tensors='pt', truncation=True, padding=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
    # Get the embeddings from the last hidden state of BERT (mean pooling)
    embeddings = outputs.last_hidden_state.mean(dim=1).squeeze().cpu().numpy()
    return embeddings.tolist()

def spark_transform():
    """
    Perform text transformation on the Spark DataFrame and store the results.
    """
    # Load the data
    spark_df = return_spark_df()

    # Drop any rows with missing text data
    spark_df = spark_df.dropna(subset=["text"])

    # Create a UDF to transform text into embeddings
    text_udf = udf(lambda text: transform_text(text), ArrayType(FloatType()))

    # Apply the transformation (text embeddings)
    spark_df = spark_df.withColumn("embeddings", text_udf(spark_df["text"]))

    # Optionally, save transformed data to HDFS or another storage
    output_path = '/batch-layer/processed_website_data_bert.json'
    spark_df.write.json(output_path, mode='overwrite')

    print("Data transformation complete")
    return spark_df

# Running the transformation
spark_transform()

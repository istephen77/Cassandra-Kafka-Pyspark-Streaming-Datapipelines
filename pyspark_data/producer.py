from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from pyspark.sql import DataFrame
import time
import logging
from datetime import datetime


# Variable declaration

# Cassandra database details
KEYSPACE = "ineuron"
TABLE = "employee"

# Kafka server details
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
KAFKA_TOPIC = "employee"

# Cassandra database connectivity credentials
CASSANDRA_HOST = "cassandra"
CASSANDRA_USER = "cassandra"
CASSANDRA_PASSWORD = "cassandra"

# Main log
# Log file name
LOG_FILE_NAME = f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"
# Log directory
LOG_FILE_DIR = os.path.join(os.getcwd(), "logs")
# Create folder if not available
os.makedirs(LOG_FILE_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_FILE_DIR, LOG_FILE_NAME),
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Create Spark session with Cassandra configuration
sparkSession = (SparkSession.builder
                 .config("spark.cassandra.connection.host", "cassandra")
                 .config("spark.cassandra.auth.username", "cassandra")
                 .config("spark.cassandra.auth.password", "cassandra")
                 .appName("demo").getOrCreate()
                 )

# Reading table from Cassandra DB and returning Spark DataFrame
def dataFrameFromCassandraDbTable(sparkSession: SparkSession, keyspace: str, table: str) -> DataFrame:
    df = (sparkSession.readStream
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load())
    return df

def sendDataToKafkaTopic(kafkaBootstrapServer, topicName, dataFrame: DataFrame):
    logging.info(f"Started writing data to kafka topic {topicName} and server: {kafkaBootstrapServer}")
    dataFrame = dataFrame.select(col("emp_id").cast(StringType()).alias("key"), to_json(struct("emp_id", "emp_name", "city", "state")).alias("value"))

    query = (dataFrame
             .writeStream
             .format("kafka")
             .outputMode("append")
             .option("kafka.bootstrap.servers", kafkaBootstrapServer)
             .option("topic", topicName)
             .option("failOnDataLoss", "false")
             .start())

    query.awaitTermination()

    logging.info(f"Data has been written to kafka topic: {topicName}")

if __name__ == "__main__":
    # Read data from Cassandra database
    df = dataFrameFromCassandraDbTable(sparkSession=sparkSession,keyspace=KEYSPACE,table=TABLE)

    # Showing DataFrame
    df.printSchema()
    df.show(truncate=False)

    nRows = df.count()
    columns = df.columns
    logging.info(f"{TABLE} has columns: [{columns}]")
    logging.info(f"{nRows} rows found in table: {KEYSPACE}.{TABLE}")

    if nRows == 0:
        logging.info("No data found, hence data will not be written to Kafka topic")
    else:
        sendDataToKafkaTopic(kafkaBootstrapServer=KAFKA_BOOTSTRAP_SERVER,
                             topicName=KAFKA_TOPIC,
                             dataFrame=df)



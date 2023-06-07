from confluent_kafka import Producer, Consumer, KafkaException
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_extract, col, desc
from pyspark.sql.streaming import StreamingQueryException
import time
import cProfile

class LogProcessor:
    def __init__(self, topic, file):
        self.topic = topic
        self.file = file
        self.EOF_RECEIVED = False

        self.spark = SparkSession.builder \
                        .appName("Log Processing") \
                        .getOrCreate()

        self.query = None

    def produce_data_to_kafka(self):
        conf = {
            'bootstrap.servers': '127.0.0.1:9092',
            'queue.buffering.max.messages': 1000000,
            'compression.type': 'gzip',
            'linger.ms': 500,
            'batch.size': 128 * 1024,
            'partitioner': 'consistent',
        }

        producer = Producer(conf)

        with open(self.file, 'r') as f:
            for line in f:
                producer.produce(self.topic, line.strip())
                producer.flush()
                time.sleep(0.001)  

        producer.flush()

        producer.produce(self.topic, 'EOF')
        producer.flush()

    def consume_and_process_data_from_kafka(self):
        kafkaParams = {
            "kafka.bootstrap.servers": "127.0.0.1:9092",
            "subscribe": self.topic,
            "startingOffsets": "earliest",
            "fetch.message.max.bytes": "1048576",
            "max.partition.fetch.bytes": "1048576",
            "group.id": "log_processor_group_17GB_8",
            "auto.offset.reset": "earliest"
        }

        df = self.spark \
            .readStream \
            .format("kafka") \
            .options(**kafkaParams) \
            .load()

        self.query = df.writeStream.foreachBatch(self.parse_and_process_batch).start()

        try:
            self.query.awaitTermination()
        except StreamingQueryException:
            print("Query stopped as expected")

    def parse_and_process_batch(self, df, epoch_id):
        PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
        split_df = df.selectExpr("CAST(value AS STRING) as value_str")

        if split_df.where(col('value_str') == 'EOF').count() > 0:
            self.EOF_RECEIVED = True

        if not self.EOF_RECEIVED:
            split_df = split_df.select(
                regexp_extract(col('value_str'), PATTERN, 1).alias('host'),
                regexp_extract(col('value_str'), PATTERN, 5).alias('method'),
                regexp_extract(col('value_str'), PATTERN, 8).alias('response_code')
            )

            # Write to Parquet for further processing
            split_df.write.mode("append").parquet("hdfs://localhost:9000/assignment3/output")

        if self.EOF_RECEIVED:
            self.query.stop()

            # Post-processing stats after consuming all data
            self.print_stats()

    def print_stats(self):
        # Read the processed data from Parquet file
        split_df = self.spark.read.parquet("hdfs://localhost:9000/assignment3/output")

        # Top 10 hosts
        print("Top 10 hosts:")
        top_10_hosts = split_df.groupBy('host').count().orderBy('count', ascending=False).limit(10)
        top_10_hosts.show()

        # Response code distribution
        print("Response code distribution:")
        response_code_distribution = split_df.groupBy('response_code').count().orderBy('count', ascending=False)
        response_code_distribution.show()

        # Request method distribution
        print("Request method distribution:")
        request_method_distribution = split_df.groupBy('method').count().orderBy('count', ascending=False)
        request_method_distribution.show()

    def process(self):
        self.produce_data_to_kafka()
        self.consume_and_process_data_from_kafka()


def main():
    kafka_topic = 'assignment3'
    log_file = '42MBSmallServerLog.log'
    
    processor = LogProcessor(kafka_topic, log_file)
    start_time = time.time()
    cProfile.runctx('processor.process()', globals(), locals(), 'process_profile.txt')
    end_time = time.time()
    print("Program execution time: ", end_time - start_time, "seconds")


if __name__ == "__main__":
    main()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time

# Function to update running totals
def update_totals(new_values, running_totals):
    if running_totals is None:
        running_totals = (0, 0.0)  # Initialize (total_quantity, total_sales)
    total_quantity = running_totals[0] + sum(v[0] for v in new_values)
    total_sales = running_totals[1] + sum(v[1] for v in new_values)
    return total_quantity, total_sales

# Function to detect anomalies
def detect_anomalies(rdd):
    if not rdd.isEmpty():
        anomalies = rdd.filter(lambda record: record[1][0] > 20 or record[1][1] > 5000)
        anomalies.foreach(lambda record: print(f"Anomaly Detected: {record}"))

# Main streaming logic
def main():
    # Initialize SparkContext and StreamingContext
    sc = SparkContext(appName="RealTimeTransactionMonitoring")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 1)  # Process data every second
    ssc.checkpoint("/tmp/spark_checkpoint")  # Enable checkpointing for stateful transformations

    # Read transaction data from socket
    data_stream = ssc.socketTextStream("localhost", 9999)

    # Parse incoming data
    transactions = data_stream.map(lambda line: line.split(",")) \
                               .map(lambda x: (x[0], (int(x[1]), float(x[2]))))  # (product_id, (quantity, sales))

    # Update running totals
    running_totals = transactions.updateStateByKey(update_totals)

    # Detect anomalies
    running_totals.foreachRDD(detect_anomalies)

    # Save running totals to text files
    running_totals.foreachRDD(lambda rdd: rdd.saveAsTextFile(f"./output/running_totals_{int(time.time())}"))

    # Start streaming
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()

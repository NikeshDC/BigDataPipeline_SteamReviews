#!/bin/bash

# Default values for script options
KAFKA_VERSION="3.8.0"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_home"
VENVDIR="myvenv"
PARTITION_COUNT=1
REPLICATION_FACTOR=1
TOPICS="reviews-stream summary-results"

THIS_LOGS=${0}.log
ZOOKEEPER_LOGS=zookeeper.log
KAFKA_SERVER_LOGS=kafka-server.log

# Function to display help message
function usage {
    echo "Usage: $0 [-d kafka_directory] [-p venv_directory] [-n partition_count] [-r replication_factor] [-t topics]"
    echo
    echo "  -d  Kafka and python virtual environment installation directory (default: ${KAFKA_DIR})"
    echo "  -v  Python virtual environment directory (default: ${VENVDIR})"
    echo "  -n  Number of partitions (default: ${PARTITION_COUNT})"
    echo "  -r  Replication factor (default: ${REPLICATION_FACTOR})"
    echo "  -t  Comma- or space-separated list of topics (default: \"${TOPICS}\")"
    exit 1
}

# Parse optional arguments
while getopts "d:v:n:r:t:" opt; do
  case ${opt} in
    d ) KAFKA_DIR=$OPTARG ;;
    v ) VENVDIR=$OPTARG ;;
    n ) PARTITION_COUNT=$OPTARG ;;
    r ) REPLICATION_FACTOR=$OPTARG ;;
    t ) TOPICS=$OPTARG ;;
    * ) usage ;;
  esac
done

# Normalize topics list (replace commas with spaces)
TOPICS=$(echo "$TOPICS" | sed 's/,/ /g')

# Update and install necessary packages without prompts
sudo apt update -y 
sudo apt install -y python3.12-venv default-jre wget tar 
sudo apt install -y default-jre 

# Set up a Python virtual environment and install required Python packages quietly
python3 -m venv ${VENVDIR}
source ${VENVDIR}/bin/activate
pip3 install kafka-python-ng pandas
deactivate

# Download and extract Kafka quietly
KAFKA_PATH="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
wget https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_PATH}.tgz
tar -xzf ${KAFKA_PATH}.tgz && mv ${KAFKA_PATH} ${KAFKA_DIR}

# Start ZooKeeper in the background and log output to a file
cd ${KAFKA_DIR}
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties > ${ZOOKEEPER_LOGS} 2>&1

# Wait for ZooKeeper to start
sleep 10

# Start Kafka broker in the background and log output to a file
bin/kafka-server-start.sh -daemon config/server.properties > ${KAFKA_SERVER_LOGS} 2>&1

# Wait for Kafka broker to start
sleep 10

# Create Kafka topics quietly with specified partitions and replication factor
for topic in $TOPICS; do
    bin/kafka-topics.sh --create --topic "$topic" --bootstrap-server localhost:9092 --replication-factor ${REPLICATION_FACTOR} --partitions ${PARTITION_COUNT} > /dev/null 2>&1
    echo "Created kafka topic: $topic"
done

echo "Kafka setup complete. Topics created: $TOPICS"
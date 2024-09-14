#!/bin/bash

# Default values for script options
VENV_DIR="myvenv"
SPARK_VERSION="3.5.2"
SPARK_DIR="spark_home"

# Function to display help message
function usage {
    echo "Usage: $0 [-d venv_directory] [-v spark_version]"
    echo
    echo "  -d  Python virtual environment directory (default: ${VENVDIR})"
    echo "  -v  Spark version (default: ${SPARK_VERSION})"
    exit 1
}

# Parse optional arguments
while getopts "d:v:" opt; do
  case ${opt} in
    v ) SPARK_VERSION=$OPTARG ;;
    d ) VENV_DIR=$OPTARG ;;
    * ) usage ;;
  esac
done

sudo apt update -y
sudo apt install -y python3.12-venv default-jre wget tar
sudo apt install -y default-jre

python3 -m venv ${VENV_DIR}
source ${VENV_DIR}/bin/activate
pip3 install dash plotly pandas pyspark==${SPARK_VERSION} kafka-python-ng
deactivate

wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_DIR}
cd ${SPARK_DIR}
export SPARK_HOME=`pwd`
export PYTHONPATH=$(find $SPARK_HOME/python/lib -name "*.zip" | tr '\n' ':')$PYTHONPATH



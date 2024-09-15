Steps to try out this project in AWS sandbox:

**Kafka**

--create an ec2 with t2.small & ubuntu

--add security rule to allow 9092 port for kafka producers/ consumers

--ssh to this instance

--create file kafka_server_setup.sh and upload contents

--run kafka_server_setup.sh

--activate the python virtual environment created by above script
>>>>source myvenv/bin/activate

--verify topics created
>> pgrep -f zookeeper
>> pgrep -f kafka.Kafka
>> bin/kafka-topics.sh --describe --topic summary-results --bootstrap-server localhost:9092
>> bin/kafka-topics.sh --describe --topic summary-results --bootstrap-server localhost:9092

--download the sample csv file
>> wget --no-check-certificate 'https://drive.google.com/uc?export=download&id=1Hj7PSv7BMrDkJ6jfDpEOJZBl97TTGaS9' -O sampled_steam_reviews_with_sentiment.csv

--create the file stream_generator.py and upload contents

--run the python file 'stream_generator.py' to generate streams



**Spark -- summarizer**

--create EMR cluster

--add security rule to allow ssh and 8085 port for dash in the master node of the cluster

--ssh to the master

--pip3 install dash plotly pandas pyspark==3.5.2 kafka-python-ng

--create file summarizer.py and upload contents, set kafka-servers' address (ec2 instance created in first step) inside the script

--submit job to spark to run the summarizer.py
>> spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2 summarizer.py


**Dash -- visualizer**

--create file visualizer.py and upload contents, set kafka-servers' address inside the script

--run the python file visualizer.py

--go to browser and open link to the dash application ([spark-master-address]:8085)

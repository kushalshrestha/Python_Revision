
```
# Build Docker images and start services
docker-compose up --build -d

docker-compose up

docker-compose down
```

### To run Kafka Producer to simulate real-time events
```
python event_producer.py
```

### List topics
```
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 
```
docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic real_time_events \
  --bootstrap-server broker:29092 \
  --partitions 1 \
  --replication-factor 1
```

### Listing topics
```
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Access Kafka UI
```
http://localhost:8080
```

### Restarting Kafka UI
```
docker-compose restart kafka-ui
```
=================
### Verifying servers and listening ports:
```
$ nc -zv localhost 22181
Connection to localhost port 22181 [tcp/*] succeeded!
$ nc -zv localhost 29092
Connection to localhost port 29092 [tcp/*] succeeded!
```

### Run producer
```
python producer.py
```

### Run consumer
```
python consumer.py
```

## Step 2: Bronze layer - Store Raw Events

We'll store raw data as it is received from Kafka. We'll store the events in a local file like CSV.
- Later on, we will transition this storage into something more scalable like a cloud data lake (S3, Azure Blob Storage, or Hadoop HDFS)

`To consume in a local file:`
```
python consumer_bronze.py
```


# Kafka Streams Real-Time Transaction Monitoring App

This project demonstrates a real-time transaction monitoring application built with Kafka Streams. The app processes transactions from a Kafka topic, identifies suspicious transactions (amount > $10,000), and writes them to another Kafka topic for further analysis.

---

## **Prerequisites**

1. Docker and Docker Compose installed.
2. Java 11 installed.
3. Maven installed.
4. Basic knowledge of Kafka and Kafka Streams.

---

## **Project Setup**

### **1. Clone the Repository**
```bash
# Clone the project repository
git clone <repository-url>
cd kafka-streams-app
```

### **2. Kafka Setup with Docker**

Create a `docker-compose.yml` file to set up Kafka and Zookeeper.

#### **docker-compose.yml**
```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper
```

Start Kafka:
```bash
docker-compose up -d
```

### **3. Create Kafka Topics**

Create the required topics:
```bash
# Create the transactions topic
docker exec -it <kafka-container-id> kafka-topics --create \
  --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Create the suspicious-transactions topic
docker exec -it <kafka-container-id> kafka-topics --create \
  --topic suspicious-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### **4. Build the Kafka Streams Application**

#### **Add Dependencies in `pom.xml`**
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams</artifactId>
        <version>3.5.0</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>2.0.9</version>
    </dependency>
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>2.0.9</version>
    </dependency>
</dependencies>
```

### **5. Dockerize the Application**

#### **Dockerfile**
```Dockerfile
FROM openjdk:23-jdk-slim

WORKDIR /app

COPY target/kafka-streams-app-1.0-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
```

#### **Build the Docker Image**
```bash
mvn clean package

docker build -t kafka-streams-app .
```

#### **Run the Docker Container**
```bash
docker run --network="host" kafka-streams-app
```

---

## **Testing the Application**

### **Produce Test Data**
Send transaction data to the `transactions` topic:
```bash
docker exec -it <kafka-container-id> kafka-console-producer \
  --topic transactions --bootstrap-server localhost:9092
```
Example input:
```
tx1,5000
tx2,20000
tx3,8000
tx4,15000
```

### **Consume Results**
Verify suspicious transactions on the `suspicious-transactions` topic:
```bash
docker exec -it <kafka-container-id> kafka-console-consumer \
  --topic suspicious-transactions --bootstrap-server localhost:9092 --from-beginning
```
Expected output:
```
tx2,20000
tx4,15000
```

---

## **Summary of Commands**

### **Setup Commands**
1. Start Kafka:
   ```bash
   docker-compose up -d
   ```
2. Create Kafka Topics:
   ```bash
   docker exec -it <kafka-container-id> kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   docker exec -it <kafka-container-id> kafka-topics --create --topic suspicious-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
   ```

### **Test Commands**
1. Produce Test Data:
   ```bash
   docker exec -it <kafka-container-id> kafka-console-producer --topic transactions --bootstrap-server localhost:9092
   ```
2. Consume Results:
   ```bash
   docker exec -it <kafka-container-id> kafka-console-consumer --topic suspicious-transactions --bootstrap-server localhost:9092 --from-beginning
   ```

---

## **Next Steps**

1. Expand the application to include more advanced Kafka Streams features like joins or aggregations.
2. Integrate the app with external systems using Kafka Connect.
3. Deploy the app to a Kubernetes cluster for production use.

For questions or improvements, feel free to contribute!

## KnowledgeBase
### **What is a Kafka Topic?**
A **Kafka topic** is a logical channel or category to which messages are sent (produced) by producers, and from which messages are read (consumed) by consumers in a Kafka system. Kafka topics are central to organizing and managing the flow of data in a Kafka cluster.
Think of a Kafka topic as a "mailbox" or "pipeline":
- Producers send records (messages) to a topic.
- Consumers read messages from a topic.

### **How Kafka Topics Work**
- A **topic** is essentially a log of records stored in Kafka.
- Topics are partitioned into multiple **partitions** (more on this below).
- Each message in a topic has a key (optional), a value, and metadata (like offset, timestamp, etc.).
- Messages in a topic are immutable and persist until a defined **retention period** (e.g., 7 days).

### **What are Partitions in Kafka?**
A **partition** is a smaller, distributed, and independent unit of a Kafka topic. When a topic is created, Kafka divides the topic into **partitions** to enable scalability, parallelism, and fault tolerance.
#### Key Features of Partitions:
1. **Scalability:**
    - Each partition can be stored on a different Kafka broker (server) in the Kafka cluster. This allows Kafka to distribute data for a topic across multiple brokers, leveraging available storage and processing power.

2. **Parallelism:**
    - Producers and consumers can work with partitions in parallel. Multiple producers and consumers can interact with the same topic simultaneously by working on different partitions.

3. **Fault Tolerance:**
    - Kafka replicates partitions across brokers, ensuring that even if one broker fails, the data is not lost.

4. **Order Guarantee:**
    - Kafka guarantees that messages in a single **partition** are delivered **in order** (FIFO – first in, first out) relative to a single producer.

### **How Partitions Work in Practice**
#### **1. Distribution in Kafka Cluster**
When you produce a message to a Kafka topic with 3 partitions:
- Kafka will determine which partition the message should be written to.
- This can be based on:
    - **Key-based partitioning**: If the message has a key, Kafka uses a hash function on the key to decide the partition.
    - **Round-robin partitioning**: If there is no key, Kafka assigns partitions sequentially to balance the load.

#### Example:
- Topic: `orders` with **3 partitions**.
- Partition 0 handles messages `OrderID: 1, 4, 7`
- Partition 1 handles messages `OrderID: 2, 5, 8`
- Partition 2 handles messages `OrderID: 3, 6, 9`

This means the data is distributed across the partitions within the topic.
#### **2. Parallelism**
Each partition can be consumed independently by a consumer (within a consumer group). For example:
- Topic: `orders` with 3 partitions.
- Consumer Group: `order-consumers` with 3 consumers.
    - Consumer 1 reads from Partition 0.
    - Consumer 2 reads from Partition 1.
    - Consumer 3 reads from Partition 2.

This allows **seamless horizontal scaling** as you can add more partitions and consumers to handle increased load.
#### **3. Replication for Fault Tolerance**
Each partition can have multiple **replicas** across brokers for fault tolerance. One replica is the **leader** (responsible for accepting read/write requests), and others are backups.
For example:
- Topic: `orders` with 3 partitions (`P0`, `P1`, `P2`).
- Replication Factor: 2.
- Broker 1 stores replicas:
    - P0 (Leader)
    - P1 (Follower)

- Broker 2 stores replicas:
    - P1 (Leader)
    - P2 (Follower)

- Broker 3 stores replicas:
    - P2 (Leader)
    - P0 (Follower)

Even if Broker 1 fails, Kafka elects a new leader (e.g., P0 on Broker 3).
### **Real-Life Analogy for Kafka Topics and Partitions**
#### **1. Topic as a Multi-Lane Highway**
Imagine a **Kafka topic** as a **multi-lane highway** with each **lane** being a **partition**.
- **Producers (Cars)** send data/messages down the highway.
- Each lane (partition) has its own independent flow of traffic.
- **Consumers (Toll booths)** process the data by reading cars traveling through specific lanes.

For example:
- A highway with 3 partitions will have 3 lanes:
    1. Cars (messages) may be sorted into lanes based on their origin (key-based partitioning).
    2. Toll booths (consumers) can manage one lane each, ensuring all lanes are processed efficiently.

#### **2. Partition as a Cash Register Line**
Imagine a Kafka topic as a **supermarket checkout**:
- The topic is the store, where all customers (messages) come to check out.
- Each checkout register is a **partition**.
- Customers (messages) are distributed across the registers (partitions).

In this analogy:
- If the store has 3 partitions, customers are grouped into 3 lines. Each line is processed independently.
- Multiple registers (partitions) allow parallel processing of customers.

#### **3. Kafka with a Bank System**
Let’s say you’re building a **bank transaction system** using Kafka:
- Topic: `transactions`.
- Partitions distribute data based on transaction accounts. (Messages with the **same key** go to the same partition.)

**For Example:**
- Accounts `A123` and `A456`:
    - Messages from account `A123` go to Partition 0.
    - Messages from account `A456` go to Partition 1.

- Parallelism: Different consumers process the partitions at the same time.
- Replication ensures transaction data is not lost even if a Kafka broker goes down.

### **Key Points to Remember**

| Feature | Kafka Topic | Kafka Partition |
| --- | --- | --- |
| **Definition** | A log-based channel where messages are organized. | A division of a topic for distribution, scalability, and fault tolerance. |
| **Parallelism** | A single topic can be processed in parallel via partitions. | Messages within a partition are strictly ordered. |
| **Order Guarantee** | Kafka maintains order **within a partition**, not across partitions. | Messages in each partition are FIFO (First-In-First-Out). |
| **Scalability** | Adding partitions increases the consumer and producer throughput. | Each partition can be consumed by a single consumer in the same consumer group. |
| **Fault Tolerance** | Enabled via partition replication. | Each partition’s replicas are distributed across brokers for reliability. |
### **Conclusion**
- Kafka **topics** are like logical containers for messages.
- **Partitions** are the real workhorses that divide the topic's data for parallelism, scalability, and fault-tolerance.
- In real-world setups, the number of partitions is carefully chosen based on consumer throughput, data volume, and fault tolerance needs. For example:
    - A topic with 10 partitions allows 10 consumers in the same consumer group to process data simultaneously.

This system makes Kafka a powerful messaging platform for distributed systems that deal with large-scale, real-time data processing.



               Producer
                  |
                Topic            //The topic is the physical log storage in Kafka
                  |
          Create a stream off of Topic
               StreamA
                  |
                  |
     Filter on StreamA to create Streams
        ---------------------------
        |         |               |
     Stream1     Stream2      Stream3    Stream is the logical abstraction of the flow of data through processing steps. 
       |           |              |
       |           |              |
     Puts data   Puts data    Puts data    
       to         to             to
       |           |              |
       |           |              |
     SubTopic1   SubTopic1    SubTopic1
       |           |              |
     Consumer    Consumer     Consumer
     For         For          For 
     Marketing   Fraud        Analytics
                 Detection




## Real-Life Analogy for Kafka Topics and Partitions
1. Topic as a Multi-Lane Highway
   Imagine a Kafka topic as a multi-lane highway with each lane being a partition.

Producers (Cars) send data/messages down the highway.
Each lane (partition) has its own independent flow of traffic.
Consumers (Toll booths) process the data by reading cars traveling through specific lanes.
For example:

A highway with 3 partitions will have 3 lanes:
Cars (messages) may be sorted into lanes based on their origin (key-based partitioning).
Toll booths (consumers) can manage one lane each, ensuring all lanes are processed efficiently.
2. Partition as a Cash Register Line
   Imagine a Kafka topic as a supermarket checkout:

The topic is the store, where all customers (messages) come to check out.
Each checkout register is a partition.
Customers (messages) are distributed across the registers (partitions).
In this analogy:

If the store has 3 partitions, customers are grouped into 3 lines. Each line is processed independently.
Multiple registers (partitions) allow parallel processing of customers.
3. Kafka with a Bank System
   Let’s say you’re building a bank transaction system using Kafka:

Topic: transactions.
Partitions distribute data based on transaction accounts. (Messages with the same key go to the same partition.)
For Example:

Accounts A123 and A456:

Messages from account A123 go to Partition 0.
Messages from account A456 go to Partition 1.
Parallelism: Different consumers process the partitions at the same time.

Replication ensures transaction data is not lost even if a Kafka broker goes down.
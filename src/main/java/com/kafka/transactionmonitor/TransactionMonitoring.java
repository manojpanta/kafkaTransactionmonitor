package com.kafka.transactionmonitor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class TransactionMonitoring {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-monitoring-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> transactions = builder.stream("transactions"); // this does not create topic
        // it creates a stream of data flowing through topic transactions
        // Stream is a continuously updating flow of key value records
        // It is **unbounded**, meaning it continuously updates as new data is published to the topic
        // String key and String value in this case
        // this topic "transactions" should already exists in kafka
        KStream<String, String> suspiciousTransactions = transactions.filter((key, value) -> {
            String[] fields = value.split(",");
            double amount = Double.parseDouble(fields[1]);
            return amount > 10000;
        });
        // here we are filtering the transaction stream with conditions above and creating another stream

        suspiciousTransactions.to("suspicious-transactions", Produced.with(Serdes.String(), Serdes.String()));
        // now we are writing to suspicious-transactions topic which also should already exists
        // The result of the filter operation (the processed data) needs to be stored or shared with other consumers.
        // Writing it to a Kafka topic makes it available for further downstream processing or for other consumers to access efficiently

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}


//Very important to remember
//
//#### **What is a Topic?**
//        - A **topic** in Kafka is a distributed log where messages (key-value pairs) are published and stored.
//        - Key Features:
//        - Topics are divided into **partitions**, enabling distributed storage and parallel consumption.
//        - Topics store data for a configurable retention time (e.g., 7 days) and allow multiple consumers to read the same data.
//
//        - Relationship Between Streams and Topics:
//        - **Streams consume data** from Kafka topics (source topics).
//        - **Streams produce data** to Kafka topics (sink topics).
//        - The topic is the physical log storage in Kafka, while the stream is the logical abstraction of the flow of data through processing steps.
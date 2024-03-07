package com.kafka;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Consumer {
	private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/default";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "";
    private static final String CLICKHOUSE_TABLE = "user_ch";
	
	public static void main(String[] args) {
		// Set Kafka broker properties
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList("test"));

        // Start consuming messages
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received message: key = %s, value = %s, offset = %d, partition = %d%n",
                            record.key(), record.value(), record.offset(), record.partition());
                    insertToClickHouse(record.value());
                }
            }
        } catch (SQLException e) {
			e.printStackTrace();
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
            // Close the consumer when done
            consumer.close();
        }
	}
	
	private static void insertToClickHouse(String value) throws SQLException, JsonParseException, JsonMappingException, IOException {
		
		ObjectMapper objectMapper = new ObjectMapper();
        // Parse JSON message
        UserModel dataModel = objectMapper.readValue(value, UserModel.class);

		
		Connection connection = DriverManager.getConnection(CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD);
        String[] fields = value.split(",");
        String name = dataModel.getName();
        String age = dataModel.getAge();
        String date = dataModel.getDate();

        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO user_ch (name, age, date) VALUES (?, ?, ?)")) {
            statement.setString(1, name);
            statement.setString(2, age);
            statement.setString(3, date);
            statement.executeUpdate();
        }
    }
}

package com.example.lynxlaststand.service;

import com.example.lynxlaststand.common.AppConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafKaProducerService {
	private static final Logger logger = LoggerFactory.getLogger(KafKaProducerService.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String message) {
		String jdbcUrl = "jdbc:sqlite:/C:\\eclipse-workspace\\lynx-last-stand-producer\\marketing_campaign.db";
		try {
			Connection connection = DriverManager.getConnection(jdbcUrl);
			String sql = "SELECT * FROM marketing_campaign";
			Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery(sql);

			while (result.next()) {

				String customer = result.getString("ID") + "," + result.getString("Marital_Status") + ","
						+ result.getString("Education") + "," + result.getString("Income") + ","
						+ result.getString("Year_Birth") + "," + result.getString("MntWines") + ","
						+ result.getString("MntFruits") + "," + result.getString("MntMeatProducts") + ","
						+ result.getString("MntFishProducts") + "," + result.getString("MntSweetProducts") + ","
						+ result.getString("MntGoldProds");


				logger.info(String.format("Message sent -> %s", message));
				this.kafkaTemplate.send(AppConstants.TOPIC_NAME, customer);

			}

		} catch (SQLException e) {
			System.out.println("Error connecting to SQLite database");
			e.printStackTrace();
		}
	}

}

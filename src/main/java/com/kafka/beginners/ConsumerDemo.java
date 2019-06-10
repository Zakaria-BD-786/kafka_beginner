package com.kafka.beginners;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	
	public static void main(String [] args) throws InterruptedException {
		
		String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
		
		Properties properties = new Properties();
		final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
		
		//create consumer configs
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
		
		
		
		//create consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
		
		//subscribe
		consumer.subscribe(Arrays.asList(topic));
		
		//poll for new data
		while (true) {
		ConsumerRecords<String,String> records =consumer.poll(Duration.ofMillis(1000));
		
		System.out.println("Read " +records.count() +" messages");
		  
		 for (ConsumerRecord<String,String> record:records) {
			 
			logger.info("Key :" +record.key() + "\n Value :" +record.value() + "\n Partition :" +record.partition()
			+"\n Offset :" +record.offset());
			 logger.info("Commiting offset");
			 Thread.sleep(1000);
			 
		 }
		      consumer.commitSync();
		}
		
	}

}

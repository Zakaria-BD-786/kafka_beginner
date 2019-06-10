package com.kafka.beginners;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	
		public static void main(String []args){
			
		String bootstrapservers = "127.0.0.1:9092";	
		String topic = "first_topic"; 
		String key = "key-2";
		String value = "value-1";
		// create properties	
	    Properties properties = new Properties();
	    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapservers);
	    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
	    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());	    
	    
	    //create producer
	    
	    KafkaProducer <String,String> producer = new KafkaProducer<String,String>(properties);
	    
	    //create producer record
	    
	    ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,value);
	    
	    //send record
	    
	    producer.send(record);
	    producer.close();
			
			
			
			
			
		}

}

package com.kafka.beginners;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallBack {
	public static void main(String []args){
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
		
		String bootstrapservers = "127.0.0.1:9092";	
		String topic = "first_topic"; 
		String key = "key-120";
		String value = "Hello Zak Big Data";
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
	     producer.send(record,new Callback() {
	    	@Override
	    	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	    		if (e == null) {
	    			logger.info("Received new metadata:\n" +
	    		                 "Topic: " +recordMetadata.topic() + "\n" +
	    					     "Partition: " +recordMetadata.partition() + "\n" +
	    		                 "offset: " +recordMetadata.offset() + "\n" +
	    					     "timestamp:" +recordMetadata.timestamp());
	    		}
	    			else
	    			{
	    				logger.error("Error while producing",e);
	    			}
	    		}
	    });
	    		
	    		
	     producer.close();
	
	
}
}

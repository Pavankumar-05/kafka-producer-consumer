package com.kafkatutorial.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.kafkatutorial.model.Order;
import com.kafkatutorial.model.Payment;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class OrderEventListener {
	
	ObjectMapper objectMapper = new ObjectMapper();
	//Topic consumer interested in
	/*
	@KafkaListener(topics="order-events")
	public void consume(Order order) {
		//Business Logic
		//For each record(event), this method will get executed
		System.out.println("Received order event: "+order.orderId());
	}
	*/
	
	@KafkaListener(topics= {"order-events", "payment-events"})
	public void consumeManaulMapping(ConsumerRecord<String, String> record) throws JacksonException{
		
		
		if("order-events".equals(record.topic())) {
			
			/*Now No manual mapping is required*/
			
			Order order = objectMapper.readValue(record.value(), Order.class);;
			//business logic for order
			System.out.println("Order: "+order.orderId());
			System.out.println("Order: "+order.toString());
			
			
		}else if("payment-events".equals(record.topic())) {
			
			Payment payment = objectMapper.readValue(record.value(), Payment.class);
			System.out.println("Payment: "+payment.paymentId());
			System.out.println("Payment Details: "+payment.toString());
			
		}
		
	}
	
}

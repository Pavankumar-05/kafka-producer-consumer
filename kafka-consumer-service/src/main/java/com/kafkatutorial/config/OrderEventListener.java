package com.kafkatutorial.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.kafkatutorial.model.Order;
import com.kafkatutorial.model.Payment;

import tools.jackson.databind.ObjectMapper;

@Component
public class OrderEventListener {

	ObjectMapper objectMapper = new ObjectMapper();
	// Topic consumer interested in
	/*
	 * @KafkaListener(topics="order-events") public void consume(Order order) {
	 * //Business Logic //For each record(event), this method will get executed
	 * System.out.println("Received order event: "+order.orderId()); }
	 */

	/*
	 * Manual-Mapping
	 */
//	@KafkaListener(topics= {"order-events", "payment-events"})
//	public void consumeManaulMapping(ConsumerRecord<String, String> record) throws JacksonException{
//		
//		
//		if("order-events".equals(record.topic())) {
//			
//			/*Now No manual mapping is required*/
//			
//			Order order = objectMapper.readValue(record.value(), Order.class);;
//			//business logic for order
//			System.out.println("Order: "+order.orderId());
//			System.out.println("Order: "+order.toString());
//			
//			
//		}else if("payment-events".equals(record.topic())) {
//			
//			Payment payment = objectMapper.readValue(record.value(), Payment.class);
//			System.out.println("Payment: "+payment.paymentId());
//			System.out.println("Payment Details: "+payment.toString());
//			
//		}
//		
//	}

	/*
	 * Auto-Mapping
	 */
//	@KafkaListener(topics= {"order-events", "payment-events"})
//	public void consume(ConsumerRecord<String, Object> record) {
//		
//		if("order-events".equals(record.topic())) {
//			Order order = (Order) record.value();
//			System.out.println("Order ID: "+order.orderId());
//		}else if("payment-events".equals(record.topic())) {
//			Payment payment = (Payment) record.value();
//			System.out.println("Payment ID: "+payment.paymentId());
//		}
//		
//	}

	@KafkaListener(topics = "order-events", containerFactory = "orderKafkaListenerFactory")

	public void consumeOrder(Order order) {

		// business logic

		System.out.println("Received:" + order.orderId());

	}

	@KafkaListener(topics = "payment-events", containerFactory = "paymentKafkaListenerFactory")

	public void consumePayment(Payment payment) {

		// business logic

		System.out.println("Received:" + payment.paymentId());

	}

}

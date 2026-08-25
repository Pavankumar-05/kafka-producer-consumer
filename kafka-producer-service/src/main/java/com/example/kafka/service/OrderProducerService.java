package com.example.kafka.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.example.kafka.model.Order;
import com.example.kafka.model.Payment;

@Service
public class OrderProducerService {

	//private final KafkaTemplate<String, Order> kafkaTemplate; // We can hardcode the value if we are passing only a single type
	private final KafkaTemplate<String, Object> kafkaTemplate; // We kept the value type as object as we are sending multiple types

	public OrderProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}
	
//	public void sendWithKey(Order order) {
//		
//		String key = order.orderId();
//		
//		CompletableFuture<SendResult<String, Order>> future = kafkaTemplate.send("order-events", key, order);
//		
//		future.whenComplete((result, ex) -> {
//			
//			if(ex == null) {
//				
//				System.out.println("Order event sent successfully");
//				
//				System.out.println("Partition: "+result.getRecordMetadata().partition());
//				
//				System.out.println("Offset: "+result.getRecordMetadata().offset());
//				
//			}else {
//				
//				System.out.println("Failed to send order event: "+ex.getMessage());
//				
//			}
//			
//		});
//		
//	}
	
	public void sendWithKey(Order order) {

	    String key = order.orderId();

	    CompletableFuture<SendResult<String, Object>> future =
	            kafkaTemplate.send("order-events", key, order);

	    future.whenComplete((result, ex) -> {

	        if (ex == null) {

	            System.out.println(
	                    "Kafka SUCCESS - Order event sent successfully"
	            );

	            System.out.println(
	                    "Partition: " +
	                    result.getRecordMetadata().partition()
	            );

	            System.out.println(
	                    "Offset: " +
	                    result.getRecordMetadata().offset()
	            );

	        } else {

	            System.out.println(
	                    "Kafka FAILURE - Failed to send order event: "
	                    + ex.getMessage()
	            );
	        }
	    });
	}
	
	public void sendWithoutKey(Order order) {
		
		 for (int i = 0; i < 32800; i++) {

			 Order modifiedOrder = new Order(
		                order.orderId() + "-" + i,
		                order.customerId(),
		                order.productId(),
		                order.quantity(),
		                order.totalAmount(),
		                order.status()
		        );

		CompletableFuture<SendResult<String, Object>> future= kafkaTemplate.send("order-events", order);		
		
		future.whenComplete((result, ex) -> {
			
			if(ex == null) {
				
				System.out.println("Order event sent successfully WITHOUT KEY");
				
				System.out.println("ORDER ID: "+order.orderId());
				
				System.out.println("Partition: "+result.getRecordMetadata().partition());
				
				
				System.out.println("Offset: "+result.getRecordMetadata().offset());
							
			}
			else {
				
				System.out.println("Failed to send order event: "+ex.getMessage());

			}
		});
		 }
		
	}

	public void sendWithKeyPayment(Payment payment) {
		
		String key = payment.paymentId();
		
		CompletableFuture<SendResult<String, Object>> response = kafkaTemplate.send("payment-events", key, payment);
		
		response.whenComplete((result, ex) -> {
			
			if(ex == null) {
				System.out.println("Payment event sent successfully!!!");
				System.out.println("Partition: "+result.getRecordMetadata().partition());
				System.out.println("Offset "+result.getRecordMetadata().offset());
			}else {
				System.out.println("Failed to send payment event: "+ex.getMessage());
			}
			
		});
		
	}

	
	
	
	
}

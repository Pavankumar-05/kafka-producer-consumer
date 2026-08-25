package com.example.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.model.Order;
import com.example.kafka.model.Payment;
import com.example.kafka.service.OrderProducerService;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

	@Autowired
	private OrderProducerService orderProducerService;
	
//	@PostMapping("/with-key")
//	public ResponseEntity<String> sendWithKey(@RequestBody Order order){
//		
//		orderProducerService.sendWithKey(order);
//		
//		return ResponseEntity.accepted().body("Order created and event published!");
//		
//	}
	
	@PostMapping("/with-key")
	public ResponseEntity<String> sendWithKey(@RequestBody Order order) {

	    orderProducerService.sendWithKey(order);

	    return ResponseEntity
	            .accepted()
	            .body("Order accepted for Kafka publishing");
	}
	
	@PostMapping("/without-key")
	public ResponseEntity<String> sendWithoutKey(
	        @RequestBody Order order) {

	    orderProducerService.sendWithoutKey(order);

	    return ResponseEntity
	            .accepted()
	            .body("Order event published WITHOUT key");
	}
	
	@PostMapping("/bulk")
	public ResponseEntity<String> sendBulkOrders(
	        @RequestParam(defaultValue = "100") int count) {

	    for (int i = 1; i <= count; i++) {

	        Order order = new Order(
	                "ORD-" + i,
	                "CUST-" + i,
	                "PROD-" + i,
	                1,
	                100.0,
	                "CREATED"
	        );

	        orderProducerService.sendWithKey(order);
	    }
	    
	    return ResponseEntity.accepted()
	            .body(count + " orders sent");
	}
	
	@PostMapping("/duplicate")
	public ResponseEntity<String> sendDuplicateOrder() {

	    Order order = new Order(
	            "ORD-1",
	            "CUST-1",
	            "PROD-1",
	            1,
	            100.0,
	            "CREATED"
	    );

	    for (int i = 0; i < 5; i++) {
	        orderProducerService.sendWithKey(order);
	    }

	    return ResponseEntity.accepted()
	            .body("Same order sent 5 times");
	}
	
	
	@PostMapping("/with-key-payment")
	public ResponseEntity<String> sendWithKeyPayment(@RequestBody Payment payment){
		
		orderProducerService.sendWithKeyPayment(payment);
		return ResponseEntity.accepted().body("Payment created and event published!");
		
	}
	
}

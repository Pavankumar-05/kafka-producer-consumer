package com.example.kafka.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.example.kafka.model.Order;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

public class OrderSummarySerializer implements Serializer<Order> {

	private final ObjectMapper objectMapper;
	
	public OrderSummarySerializer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public byte[] serialize(String topic, Order order) {
		if(order == null)
			return null;
		
		try {
			//Build a map with only the fields we want to expose, rest I removed it
			Map<String, Object> summary = new LinkedHashMap<>();
			summary.put("orderId", order.orderId());
			summary.put("productId", order.productId());
			return objectMapper.writeValueAsBytes(summary);
			
		}catch(JacksonException ex) {
			throw new RuntimeException("Failed to serialize Order summary", ex);
		}
		catch(Exception ex) {
			throw new RuntimeException("Failed to serialize Order summary", ex);
	}

}

	
	
}

package com.kafkatutorial.model;

public record Order(String orderId, String customerId, String productId, Integer quantity, Double totalAmount, String status) {

}

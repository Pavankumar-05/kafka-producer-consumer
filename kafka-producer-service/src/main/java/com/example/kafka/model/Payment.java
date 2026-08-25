package com.example.kafka.model;

public record Payment(String paymentId, String customerId, Double totalAMount, String status) {

}

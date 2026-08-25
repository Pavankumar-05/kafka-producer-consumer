package com.kafkatutorial.model;

public record Payment(String paymentId, String customerId, Double totalAMount, String status) {

}

package com.kafkatutorial.config;

import java.util.Map;

import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import com.kafkatutorial.model.Order;
import com.kafkatutorial.model.Payment;

@Configuration
public class ConsumerConfig {

	@Bean
	public ConsumerFactory<String, Order> orderConsumerFactory(KafkaProperties props) {

		Map<String, Object> config = props.buildConsumerProperties();
		config.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, Order.class);
		return new DefaultKafkaConsumerFactory<>(config);

	}

	@Bean
	public ConsumerFactory<String, Payment> paymentConsumerFactory(KafkaProperties props) {

		Map<String, Object> config = props.buildConsumerProperties();
		config.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, Payment.class);
		return new DefaultKafkaConsumerFactory<>(config);

	}

	@Bean

	public ConcurrentKafkaListenerContainerFactory<String, Order> orderKafkaListenerFactory(
			ConsumerFactory<String, Order> orderConsumerFactory) {

		ConcurrentKafkaListenerContainerFactory<String, Order> factory = new ConcurrentKafkaListenerContainerFactory<>();

		factory.setConsumerFactory(orderConsumerFactory);

		return factory;

	}

	@Bean

	public ConcurrentKafkaListenerContainerFactory<String, Payment> paymentKafkaListenerFactory(
			ConsumerFactory<String, Payment> paymentConsumerFactory) {

		ConcurrentKafkaListenerContainerFactory<String, Payment> factory = new ConcurrentKafkaListenerContainerFactory<>();

		factory.setConsumerFactory(paymentConsumerFactory);

		return factory;

	}

}

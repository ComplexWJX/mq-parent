package com.jaxon.kafka.spring.autoconfig;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;

/**
 * Created with IntelliJ IDEA.
 *
 * @author WJX
 * @version 1.0
 * @date 2024/01/17/14:39
 * @description
 */
@Configuration
@EnableConfigurationProperties(KafkaProperties.class) // 相当于使@Configuration注解的类注入
//@Import()
public class KafkaCustomConfiguration {

    private final KafkaProperties properties;

    public KafkaCustomConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    @ConditionalOnMissingBean(KafkaTemplate.class)
    public KafkaTemplate<?, ?> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory,
                                             ProducerListener<Object, Object> kafkaProducerListener,
                                             ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setDefaultTopic(this.properties.getTemplate().getDefaultTopic());
        return kafkaTemplate;
    }

    @Bean
    @ConditionalOnMissingBean(ProducerFactory.class)
    public ProducerFactory<Object, Object> kafkaProducerFactory(ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {
        DefaultKafkaProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(
                properties.buildProducerProperties());
        String transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            producerFactory.setTransactionIdPrefix(transactionIdPrefix);
        }
        customizers.stream().forEach(customizer -> customizer.customize(producerFactory));
        return producerFactory;
    }
}

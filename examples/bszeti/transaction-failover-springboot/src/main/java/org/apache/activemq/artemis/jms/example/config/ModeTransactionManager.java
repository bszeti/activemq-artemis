package org.apache.activemq.artemis.jms.example.config;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.artemis.jms.example.TransactionFailoverSpringBoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.connection.JmsTransactionManager;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

@Configuration
@ConditionalOnProperty(
    value = "transaction.mode",
    havingValue = "JMS_TRANSACTION_MANAGER")
@EnableJms
public class ModeTransactionManager {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    TransactionFailoverSpringBoot app;

//
//    @Bean
//    public PlatformTransactionManager transactionManager(ConnectionFactory connectionFactory) {
//        return new JmsTransactionManager(connectionFactory);
//
//    }

    @Bean
    public JmsListenerContainerFactory<?> msgFactory(ConnectionFactory connectionFactory
//                                                     ,PlatformTransactionManager transactionManager
    ) {
        log.info("Custom JmsListenerContainerFactory is created.");
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setTransactionManager(new JmsTransactionManager(connectionFactory));
        factory.setConnectionFactory(connectionFactory);
        factory.setSessionTransacted(false); //this line has no impact as session will be transacted if TransactionManager is set.
        factory.setReceiveTimeout(5000L);
        return factory;
    }

    @JmsListener(destination = "${source.queue}", concurrency = "${receive.concurrentConsumers}", containerFactory = "msgFactory")
    public void receiveMessageWithTransactionManager(String text, Session session, @Header("SEND_COUNTER") String counter, @Header("UUID") String uuid) throws InterruptedException {
        app.doReceiveMessage(text, session, counter, uuid);
    }
}

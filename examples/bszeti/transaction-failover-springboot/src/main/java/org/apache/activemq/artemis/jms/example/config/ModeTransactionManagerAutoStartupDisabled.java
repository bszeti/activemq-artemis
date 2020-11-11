package org.apache.activemq.artemis.jms.example.config;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.artemis.jms.example.TransactionFailoverSpringBoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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


@Configuration
@ConditionalOnProperty(
    value = "transaction.mode",
    havingValue = "JMS_TRANSACTION_MANAGER_AUTOSTARTUP_DISABLED")
@EnableJms
public class ModeTransactionManagerAutoStartupDisabled {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    TransactionFailoverSpringBoot app;

    @Value("${receive.cacheLevel}")
    String cacheLevel;

    @Bean
    public PlatformTransactionManager transactionManager(ConnectionFactory connectionFactory) {
        return new JmsTransactionManager(connectionFactory);

    }

    @Bean
    public JmsListenerContainerFactory<?> msgFactory(ConnectionFactory connectionFactory
                                                     ,DefaultJmsListenerContainerFactoryConfigurer configurer
                                                     ,PlatformTransactionManager transactionManager ) {
        log.info("Custom JmsListenerContainerFactory is created.");
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setTransactionManager(transactionManager); //setSessionTransacted=true is set by initialize() if TransactionManager is set
        factory.setConnectionFactory(connectionFactory);
        factory.setCacheLevelName(cacheLevel); //Default is CACHE_NONE if TransactionManager is set, CACHE_CONSUMER otherwise
        factory.setReceiveTimeout(5000L);

        factory.setAutoStartup(false); //autoStartup=false causes duplicates at the end! Default is "true"
        //An autowired DefaultJmsListenerContainerFactoryConfigurer.configure(factory, connectionFactory) sets autoStartup=false
        //configurer.configure(factory, connectionFactory); //Sets autoStartup=false that causes duplicates.

        return factory;
    }

    @JmsListener(destination = "${source.queue}", concurrency = "${receive.concurrentConsumers}", containerFactory = "msgFactory")
    public void receiveMessageWithTransactionManager(String text, Session session, @Header("SEND_COUNTER") String counter, @Header("UUID") String uuid) throws InterruptedException {
        app.doReceiveMessage(text, session, counter, uuid);
    }
}

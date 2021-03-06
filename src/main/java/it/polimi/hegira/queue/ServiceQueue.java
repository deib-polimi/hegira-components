/**
 * Copyright 2015 Marco Scavuzzo
 * Contact: Marco Scavuzzo <marco.scavuzzo@polimi.it>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.polimi.hegira.queue;

import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.utils.DefaultSerializer;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Service queue which allows the interaction between the API component and the SRC or TWC
 * @author Marco Scavuzzo
 *
 */
public class ServiceQueue {
	private static Logger log = Logger.getLogger(ServiceQueue.class);

	private ConnectionFactory factory;
	private Connection connection;
	private Channel channelPublish;
	private Channel channelConsume;
	private static final String EXCHANGE_NAME = "service-queue";
	public static final String API_PUBLISH_RK = "toApiServer";
	private String LISTEN_RK;
	private QueueingConsumer consumer;
	
	/**
	 * Creates a service queue between the given component and hegira-api.
	 * @param componentType The type of component creating the queue, i.e. SRC or TWC.
	 * @param queueAddress The address of RabbitMQ broker.
	 */
	public ServiceQueue(String componentType, String queueAddress){
		LISTEN_RK = componentType;
		
		factory = new ConnectionFactory();
		factory.setHost(queueAddress);
		try {
			connection = factory.newConnection();
			//SENDER PART
			channelPublish = connection.createChannel();
			/**
			 * direct exchange: a message goes to the queues whose binding key 
			 * exactly matches the routing key of the message.
			 */
			channelPublish.exchangeDeclare(EXCHANGE_NAME, "direct");
			log.debug("Publish channel created. Exchange: "+EXCHANGE_NAME+" type: direct");
			
			//RECEIVER PART
			channelConsume = connection.createChannel();
			channelConsume.exchangeDeclare(EXCHANGE_NAME, "direct");
			log.debug("Consuming channel created. Exchange: "+EXCHANGE_NAME+" type: direct");
			
			//String queueName = channelConsume.queueDeclare().getQueue();
			/**
			 * queueDeclare(java.lang.String queue, boolean durable, 
			 * boolean exclusive, boolean autoDelete, 
			 * java.util.Map<java.lang.String,java.lang.Object> arguments) 
			 */
			String queueName;
			switch(componentType){
				case "SRC":
					queueName = channelConsume.queueDeclare("Q2", false, false, false, null).getQueue();
					break;
				case "TWC":
					queueName = channelConsume.queueDeclare("Q3", false, false, false, null).getQueue();
					break;
				default:
					queueName = channelConsume.queueDeclare().getQueue();
					break;
			}

			/**
			 * routing key bindings: relationship between an exchange and a queue.
			 * Binds the queue, just created, with an exchange and with a routing key.
			 */
			channelConsume.queueBind(queueName, EXCHANGE_NAME, LISTEN_RK);
			log.debug("Binding the consuming channel. ROUTING KEY: "+LISTEN_RK+" QUEUE NAME: "+queueName);
			
			/**
			 * Telling the server to deliver us the messages from the queue. 
			 * Since it will push us messages asynchronously, we provide a callback 
			 * in the form of an object (QueueingConsumer) that will buffer the messages 
			 * until we're ready to use them.
			 */
			consumer = new QueueingConsumer(channelConsume);
			/**
			 * basicConsume(java.lang.String queue, boolean autoAck, Consumer callback)
			 * Starts a non-nolocal, non-exclusive consumer, 
			 * with a server-generated consumerTag.
			 */
			channelConsume.basicConsume(queueName, true, consumer);
			log.debug("Consumer started on queue: "+queueName);
		} catch (IOException e) {
			log.error(e.toString());
		}
	}
	
	/**
	 * Writes a message in the queue with the given routing key.
	 * @param routingKey The routing key.
	 * @param messageBody The message to be sent.
	 * @throws QueueException if an error has occurred.
	 */
	public void publish(String routingKey, byte[] messageBody) throws QueueException{
		/**
		 * Declaring a queue is idempotent - it will only be created if it doesn't exist already. 
		 * basicPublish(java.lang.String exchange, 
		 * 		java.lang.String routingKey, 
		 * 		AMQP.BasicProperties props, byte[] body)
		 */
		try {
			//declare Q1 regardless if it already exists or not ...
			String queueName = channelConsume.queueDeclare("Q1", false, false, false, null).getQueue();
			//... and bind it to the proper Exchange and routingKey
			channelPublish.queueBind(queueName, EXCHANGE_NAME, routingKey);
			
			channelPublish.basicPublish(EXCHANGE_NAME, routingKey, null, messageBody);
			log.debug("Message published. ROUTING KEY: "+routingKey);
		} catch (IOException e) {
			throw new QueueException();
		}
	}
	
	/**
	 * Used by the SRC or the TWC to announce their presence to hegira-api component.
	 * @throws QueueException
	 */
	public void announcePresence() throws QueueException{
		publish(API_PUBLISH_RK, LISTEN_RK.getBytes());
	}
	
	/**
	 * Listens for commands sent by hegira-api component
	 * @return
	 * @throws QueueException
	 */
	public ServiceQueueMessage receiveCommands() throws QueueException{
		
		/**
		 * QueueingConsumer.nextDelivery() blocks 
		 * until another message has been delivered from the server.
		 */
		QueueingConsumer.Delivery delivery;
		try {
			log.debug("waiting for messages");
			delivery = consumer.nextDelivery();
			byte[] message = delivery.getBody();
        		//String routingKey = delivery.getEnvelope().getRoutingKey();
        		
        		try{
				ServiceQueueMessage sqm = (ServiceQueueMessage) DefaultSerializer.deserialize(message);
				return sqm;
				
        		} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
        			
				e.printStackTrace();
			}
        		
        		
		} catch (ShutdownSignalException | ConsumerCancelledException
				| InterruptedException e) {
			throw new QueueException();
		}
		
		return null;	
		
	}
	
	/**
	 * Gets queuing consumer.
	 * @return The queuing consumer.
	 */
	public QueueingConsumer getConsumer(){
		return consumer;
	}
	
}

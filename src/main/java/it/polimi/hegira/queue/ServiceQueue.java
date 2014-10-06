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
	QueueingConsumer consumer;
	
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
			
			String queueName = channelConsume.queueDeclare().getQueue();
			/**
			 * routing key bindings: relationship between an exchange and a queue.
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
	 * @param routingKey
	 * @param messageBody
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
			channelPublish.basicPublish(EXCHANGE_NAME, routingKey, null, messageBody);
			log.debug("Message published. ROUTING KEY: "+routingKey);
		} catch (IOException e) {
			throw new QueueException();
		}
	}
	
	public void announcePresence() throws QueueException{
		publish(API_PUBLISH_RK, LISTEN_RK.getBytes());
	}
	
	public void receiveCommands() throws QueueException{
		while(true){
			/**
			 * QueueingConsumer.nextDelivery() blocks 
			 * until another message has been delivered from the server.
			 */
			QueueingConsumer.Delivery delivery;
			try {
				delivery = consumer.nextDelivery();
				byte[] message = delivery.getBody();
            		String routingKey = delivery.getEnvelope().getRoutingKey();
            		
            		try{
					ServiceQueueMessage sqm = (ServiceQueueMessage) DefaultSerializer.deserialize(message);
					switch(sqm.getCommand()){
						case "switchover":
							if(LISTEN_RK.equals("SRC")){
		            				
			            		}else if(LISTEN_RK.equals("TWC")){
			            			
			            		}
							break;
						default:
							break;
					}
					
            		} catch (ClassNotFoundException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            		
            		
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException e) {
				throw new QueueException();
			}	
		}
	}
	
	public QueueingConsumer getConsumer(){
		return consumer;
	}
	
}

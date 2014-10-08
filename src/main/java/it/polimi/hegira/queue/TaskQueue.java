package it.polimi.hegira.queue;

import it.polimi.hegira.exceptions.QueueException;
import it.polimi.hegira.utils.Constants;
import it.polimi.hegira.utils.DefaultErrors;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

/**
 * Task queue used by:
 * 	1. the SRC to write Metamodel entities;
 *  2. the TWC to read Metamodel entities;
 *  
 * @author Marco Scavuzzo
 */
public class TaskQueue {
	private static Logger log = Logger.getLogger(TaskQueue.class);

	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private QueueingConsumer consumer;
	protected static final String TASK_QUEUE_NAME = "task_queue";
	
	protected int THREADS_NO = 10;
	private int MAX_THREADS_NO = 60;

	public TaskQueue(String mode, int threads, String queueAddress) throws QueueException{
		if(mode==null) return;
		
		factory = new ConnectionFactory();
		if(queueAddress==null || queueAddress.isEmpty()){
			factory.setHost("localhost");
		}else{
			factory.setHost(queueAddress);
		}
		
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			/**
			 * Declaring a durable queue
			 * queueDeclare(java.lang.String queue, boolean durable, 
			 * boolean exclusive, boolean autoDelete, 
			 * java.util.Map<java.lang.String,java.lang.Object> arguments) 
			 */
			channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
			
			 //queue settings differentiation
			switch(mode){
				case Constants.PRODUCER:
					consumer=null;
					break;
				case Constants.CONSUMER:
					if(threads>10 && threads <= 60){
						this.THREADS_NO = threads;
					}else if(threads>60){
						this.THREADS_NO = MAX_THREADS_NO;
						log.info(DefaultErrors.getThreadsInformation(MAX_THREADS_NO));
					}
					
					channel.basicQos(1);
					consumer = new QueueingConsumer(channel);
					/**
					 * basicConsume(java.lang.String queue, boolean autoAck, Consumer callback)
					 * Starts a non-nolocal, non-exclusive consumer, 
					 * with a server-generated consumerTag.
					 */
					channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
					break;
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			throw new QueueException(e.getMessage());
		}
	}
	
	/**
	 * Publishes a message in the task queue
	 * @param message The message to be published
	 * @throws QueueException if an error is encountered
	 */
	public void publish(byte[] message) throws QueueException{
		try {
			/**
			 * void basicPublish(java.lang.String exchange,
	         *       java.lang.String routingKey,
	         *       AMQP.BasicProperties props,
	         *       byte[] body)
			 */
			channel.basicPublish("", TASK_QUEUE_NAME, null, message);
		} catch (IOException e) {
			throw new QueueException(e.getMessage(), e.getCause());
		}
	}
	
	/**
	 * Acknowledge a message given its delivery tag.
	 * @param deliveryTag
	 */
	public void sendAck(long deliveryTag) throws QueueException{
		try {
			channel.basicAck(deliveryTag, false);
		} catch (IOException e) {
			throw new QueueException(e.getMessage(),e.getCause());
		}
	}
	
	/**
	 * Acknowledge a message given its delivery (returned from the QueuingConsumer object).
	 * @param delivery
	 * @throws QueueException
	 */
	public void sendAck(Delivery delivery) throws QueueException{
		try {
			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		} catch (IOException e) {
			throw new QueueException(e.getMessage(),e.getCause());
		}
	}
	
	public QueueingConsumer getConsumer(){
		return consumer;
	}
}

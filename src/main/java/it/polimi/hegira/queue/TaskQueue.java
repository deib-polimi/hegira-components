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
	
	/**
	 * Returns an approxiamation of the messages present in the queue.
	 * NB. Sometimes the count may be 0.
	 * @param queue_name The name of the queue to query.
	 * @return	The number of messages in the queue.
	 */
	public int getMessageCount(String queue_name){
		try {
			return channel.queueDeclarePassive(queue_name).getMessageCount();
		} catch (IOException e) {
			log.debug("Error reading message count from "+queue_name, e);
			return 0;
		}
	}
	
	public QueueingConsumer getConsumer(){
		return consumer;
	}
	
	public static String getDefaultTaskQueueName(){
		return TASK_QUEUE_NAME;
	}
	
	public void slowDownProduction(){
		int queueElements = 0;
		long previousQueueCheckTime=0;
		
		if(queueElements>0 && previousQueueCheckTime>0){
			
			int messageCount = getMessageCount(TASK_QUEUE_NAME);
			
			//Producer is faster then consumers
			if(messageCount-queueElements>0 && messageCount>50000){
				long consumingRate = (messageCount-queueElements)/
						(System.currentTimeMillis() - previousQueueCheckTime);
			
				if(consumingRate<=0) consumingRate=1;
				/*
				 * How much time should I wait to lower the queue to 50'000entities?
				 * t = (messageCount(ent) - 50000(ent))/consumingRate(ms)
				 * Anyway, wait no more than 40s
				 */
				long t = (messageCount - 50000)/consumingRate;
				if(t>40000) t=40000;
				if(t<0) t=0;
				
				log.debug("Consuming rate: "+consumingRate+"ent/ms. \tSlowing down ... wait "+t+" ms");
				//Thread.currentThread().wait(t);
				try {
					Thread.sleep(t);
				} catch (InterruptedException e) {
					log.error("Cannot puase", e);
				}
				
			}
		
			queueElements = getMessageCount(TASK_QUEUE_NAME);
			previousQueueCheckTime = System.currentTimeMillis();
			
		}
	}
}

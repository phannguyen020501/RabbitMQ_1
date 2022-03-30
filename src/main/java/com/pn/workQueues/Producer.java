package com.pn.workQueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Producer implements Runnable{
    private final static String QUEUE_NAME = "pn_queue";

    private String name;
    private int numOfMessage;

    public Producer(String name, int numOfMessage) {
        this.name = name;
        this.numOfMessage = numOfMessage;
    }

    @Override
    public void run() {
        System.out.println("create a connectionFactory for " + name);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()){
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            System.out.println("start sending messages....");
            int index = 1;
            while(index <= numOfMessage){
                String message = " Task #" + index++;
                channel.basicPublish("", QUEUE_NAME, null,
                        message.getBytes(StandardCharsets.UTF_8));
                System.out.println("[x] "+ name + " Sent: " + message +" ");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }finally {
            System.out.println("Close connection and free resources");
        }
    }
}

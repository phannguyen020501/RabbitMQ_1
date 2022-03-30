package com.pn.workQueues;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer implements Runnable{
    private final static String QUEUE_NAME = "pn_queue";
    private int numberConsumedMessage = 0;
    private  String name;
    private int timeToFinishATask;

    public Consumer(int numberConsumedMessage, String name, int timeToFinishATask) {
        this.numberConsumedMessage = numberConsumedMessage;
        this.name = name;
        this.timeToFinishATask = timeToFinishATask;
    }

    public Consumer(String name, int timeToFinishATask){
        this.name = name;
        this.timeToFinishATask = timeToFinishATask;
    }
    @Override
    public void run() {
        try{
            System.out.println("create a connectionFactory for "+ name);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");

            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            channel.basicQos(1);
            //mặc định rabbitmq: sử dụng round-robin để gửi message đến consumer 1 cách tuần tự
            // mỗi consumer sẽ có thời gian xử lý mối task khác nhau.để tránh 1 consumer nhận quá nhiều
            // mà không có thời gian xử lý hay 1 consumer quá rảnh k có thời gian để thực hiện
            // sử dụng option basicQos(): chỉ gửi 1 message cho consumer, khi nào xử lý xong hãy gửi tiếp.
            // nhờ vậy mà thời gian xử lý nhanh hơn

            System.out.println("start receving messages ...");
            DeliverCallback deliverCallback = (consumerTag, delivery) ->{
                String message= new String(delivery.getBody(), "UTF-8");
                System.out.println(" " + name + " Received: " + message+ " ");
                consume(message);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println("[-] "+ name + " Already consumed: "
                        + (++numberConsumedMessage) + " Task");

            };
            CancelCallback cancelCallback = consumerTag -> {};
            boolean autoAck = false;
            String consumerTag = channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, cancelCallback);
            System.out.println("Tag for " + name + ": " +consumerTag);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
    private void consume(String message){
        try{
            Thread.sleep(timeToFinishATask);
            System.out.println("[-] " + name + " Consumed for " +
                    message + " in " + timeToFinishATask+ " ms");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

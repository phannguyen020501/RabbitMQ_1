package com.pn.amqp;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private final static String  QUEUE_NAME = "queue_1";

    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("create a ConnectionFactory");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

//        factory.setHost("192.168.6.162");
//        factory.setPort(5672);
//        factory.setUsername("vsat");
//        factory.setPassword("vsat@2021");

        System.out.println("create a connection");
        System.out.println("create a channel");
        Connection connection =factory.newConnection();

        Channel channel = connection.createChannel();

        System.out.println("create a queue "+ QUEUE_NAME);
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        // tương tự producer: tạo 1 queue để lưu trữ các message
        System.out.println("waiting for message. to exit press Ctrl + C");

        System.out.println("start receiving messages...");
        DeliverCallback deliverCallback = (consumerTag, delivery)-> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("received: " + message + " ");
        };

        CancelCallback cancelCallback = consumerTag -> {};
        String consumerTag = channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback);
        //nhận message từ queue. các đối số
        /*String queue: tên của queue muốn nhận message từ
        - bool autoAck: tự động gửi lại 1 ack message đến rabbitmq để báo rằng có 1 message đã dc consumer nhận
        xử lý và rabbit có thể xóa nó
        - DeliverCallBack: cung cấp 1 callback để xử lý khi một message đến
        - CancelCallBack: cung cấp 1 callback để xử lý khi một consumer bị cancel vì 1 lý do nào đó, chẳng hạn
        queue bị cancel
         */
        System.out.println("consumerTag: " + consumerTag);

    }
}

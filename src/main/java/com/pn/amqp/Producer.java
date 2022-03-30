package com.pn.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Producer {
    private final static String QUEUE_NAME = "queue_1";

    public static void main(String[] args) {
        System.out.println("create a connectionFactory");
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost("localhost");
//        factory.setHost("192.168.6.162");
//        factory.setPort(5672);
//        factory.setUsername("vsat");
//        factory.setPassword("vsat@2021");

        System.out.println("create a connection");
        System.out.println("create a channel");
        try (Connection connection = factory.newConnection()){
            Channel channel = connection.createChannel();
            System.out.println("create queue "+ QUEUE_NAME);
            channel.queueDeclare(QUEUE_NAME,false, false, false, null);
            //tạo một queue để lưu trữ các message, các đối số
            /*
            * String name: tên của queue muốn gửi message đến
            * bool durable: queue và message sẽ bị xóa khi rabbitmq server stop, để giữ lại thì set true
            * bool exclusive: queue sẽ bị xóa khi connection close
            * book autoDelete: queue sẽ bị xóa khi consumer cuối cùng của nó bị hủy hoặc đóng hoặc mất kết nối
            * map<String, object> argumens: các tham số nếu có
            * */

            System.out.println("start sending message");
            try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in))){
                String message;
                do {
                    System.out.println("Enter message: ");
                    message = br.readLine().trim();
                    //trim(): xóa khoảng trắng

                    channel.basicPublish("", QUEUE_NAME, null,
                            message.getBytes(StandardCharsets.UTF_8));
                    // gửi message lên exchange
                    /*
                    * String exchange: tên exchange: không có là empty-> default exchange
                    * String routingKey: key của exchange
                    * basicProperties: props: các thuộc tính khác cho message
                    * byte[] body: nội dụng của tin nhắn
                    * */
                    System.out.println("Sent: " + message+ " ");

                }while (!message.equalsIgnoreCase("close"));
            }finally {
                System.out.println("close connection and free resources");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }
}

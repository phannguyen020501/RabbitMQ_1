package com.pn.workQueues;

public class App {
    public static void main(String[] args) {
        Producer producer1 = new Producer("[Producer 1]", 10);
        Consumer consumer1 = new Consumer("[Consumer 1]", 100);
        Consumer consumer2 = new Consumer("[Consumer 2]", 300);

        new Thread(producer1).start();
        new Thread(consumer1).start();
        new Thread(consumer2).start();

    }
}

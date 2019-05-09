package com.aureum.stream.flinkfilter.producer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import javax.annotation.PostConstruct;

@Component
public class DataStreamProducer {
    private final Logger logger = LoggerFactory.getLogger(DataStreamProducer.class);
    private ServerSocket serverSocket;
    private DataOutputStream dos;
    private Socket socket;

    @Autowired
    private TaskExecutor taskExecutor;

    public DataStreamProducer(){
        try {
            serverSocket = new ServerSocket(6777);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @PostConstruct
    public void initProducer(){
        taskExecutor.execute(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try (Socket socket1 =  serverSocket.accept()){
                        dos = new DataOutputStream(socket1.getOutputStream());
                        Random random = new Random();
                        String str = String.valueOf(random.nextInt(9999))+" ";
                        dos.writeBytes(str);
                        logger.info("Stream random # " +str);
                    //} catch (InterruptedException e) {
                        //e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        });

    }

}

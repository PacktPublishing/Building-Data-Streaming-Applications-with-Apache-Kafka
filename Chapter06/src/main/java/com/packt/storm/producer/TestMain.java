package com.packt.storm.producer;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Created by chanchal.singh on 6/15/2017.
 */
public class TestMain {
    public static void main(String[] args) {
        IPLogProducer ipLogProducer = new IPLogProducer();

        BufferedReader br = ipLogProducer.readFile();
        String oldLine = "";
        try {
            while ((oldLine = br.readLine()) != null) {
                System.out.println(oldLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

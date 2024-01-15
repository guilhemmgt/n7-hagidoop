package tests;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

import config.*;
import interfaces.KV;

public class TestProject {

    public static void main(String[] args) {
        // testConfig();
        testMachineName();
    }

    public static void testConfig() {
        try {
            List<KV> lkv = Project.getConfig("config.txt");

            for (KV kv : lkv) {
                System.out.println(kv.toString());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void testMachineName() {
        try {
            InetAddress inetadd = InetAddress.getLocalHost();
            String name = inetadd.getHostName();
            String address = inetadd.getHostAddress();
            System.out.println("HostName is : " + name);
            System.out.println("Host Address is: " + address);
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
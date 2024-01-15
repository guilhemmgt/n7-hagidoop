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
        // testMachineName();
        testSplit();
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
            System.out.println("HostName is : " + name);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void testSplit() {
        String testString = "vador.enseeiht.fr";
        System.out.println(testString.split("\\.").toString());
    }
}
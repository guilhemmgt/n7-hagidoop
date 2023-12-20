package tests;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

import config.*;
import interfaces.KV;

public class TestProject {

    public static void main(String[] args) {
        testConfig();
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
}
package hdfs;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.*;

public class HdfsServer {
    private Socket s;
    private ServerSocket ss;

    public HdfsServer(Socket s) {
        this.s = s;
    }

    public void main(String[] args) throws IOException {
        ss = new ServerSocket(Integer.parseInt(args[0]));
        while (true) {
            s = ss.accept();
            Slave slave = new Slave(s);
            slave.start();
        }
    }
}

class Slave extends Thread {
    Socket emetteur;

    public Slave(Socket emetteur) {
        this.emetteur = emetteur;
    }

    @Override
    public void run() {
        try {
            InputStreamReader in = new InputStreamReader(emetteur.getInputStream());
            String rq = new LineNumberReader(in).readLine(); // ma ligne

            // ...
        } catch (Exception e) {

        }
    }
}

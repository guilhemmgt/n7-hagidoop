package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.*;

import interfaces.FileReaderWriter;
import interfaces.KV;
import io.AccessMode;
import io.FileReaderWriteImpl;

public class HdfsServer implements Runnable {
    private Socket s;
    private static FileReaderWriter rw = new FileReaderWriteImpl();

    public HdfsServer(Socket s) {
        this.s = s;
    }

    private static void usage() {
		System.out.println("Usage: java HdsfServer <port> <path>");
	}

    public static void Start(int port, String hdfsDirectoryPath) {
        try {
            ServerSocket ss = new ServerSocket(port);

            // Accède au fichier du fragment
            // TODO robustesse du path
            // TODO gérer le nom du fichier
            String filePath = hdfsDirectoryPath + (hdfsDirectoryPath.endsWith("\\") ? "" : "\\") + "fragment.txt";
            File file = new File(filePath);
            file.getParentFile().mkdir(); // Créer le répertoire si il n'existe pas
            file.createNewFile(); // Créer le fichier si il n'existe pas

            // Ouvre le fichier en écriture
            rw.setFname(filePath);
            
            // Écoute
            while (true) {
                new Thread(new HdfsServer(ss.accept())).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            // Récupère la ligne
            InputStreamReader in = new InputStreamReader(s.getInputStream());
            LineNumberReader lnr = new LineNumberReader(in);

            // Ouvre le ReaderWriter
            rw.open(AccessMode.WRITE);

            // Écris chaque ligne reçue (de la forme "n°_de_ligne<->ligne")
            String received;
            while ((received = lnr.readLine()) != null) {
                String[] parsed_received = received.split(KV.SEPARATOR); // [n°_de_ligne, ligne]
                KV kv_received = new KV(parsed_received[0], parsed_received[1]); // n°_de_ligne<->ligne
                rw.write(kv_received); // Écris
            }

            // Ferme le ReaderWriter
            rw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            Start (Integer.parseInt(args[0]), args[1]);
        } else {
            usage();
        }
    }
}

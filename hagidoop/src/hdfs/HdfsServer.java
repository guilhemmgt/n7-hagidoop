package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.*;
import java.nio.file.Path;
import java.nio.file.Paths;

import interfaces.FileReaderWriter;
import interfaces.KV;
import io.AccessMode;
import io.FileReaderWriteImpl;

public class HdfsServer implements Runnable {
    private Socket s;
    private static FileReaderWriter frw = new FileReaderWriteImpl(FileReaderWriter.FMT_TXT);
    private static Path pathToServerDir; // Répertoire du noeud, où il stocke ses fragments

    private HdfsServer(Socket s) {
        this.s = s;
    }

    private static void usage() {
		System.out.println("Usage: java HdsfServer <port> <path>");
	}

    private static void Start(int port) {
        try {
            ServerSocket ss = new ServerSocket(port);

            System.out.println("Worker créé. Port: " + port + ", répertoire: " + pathToServerDir);

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
            String request = lnr.readLine();

            // Identifie la requête
            if (request.startsWith(HdfsClient.WRITE_RQ)) { // Écrire un fragment dans HDFS
                // Créé le fichier du fragment et ouvre le FileReaderWriter
                SetFile(request.substring(HdfsClient.WRITE_RQ.length()));

                // Écris chaque ligne reçue (de la forme "n°_de_ligne<->ligne")
                String received;
                while ((received = lnr.readLine()) != null) {
                    String[] parsed = received.split(KV.SEPARATOR, 2); // [n°_de_ligne, ligne]
                    KV kv = new KV(parsed[0], parsed[1]); // n°_de_ligne<->ligne
                    frw.write(kv); // Écris
                }

                // Ferme le ReaderWriter
                frw.close();
            } 
            else if (request.startsWith(HdfsClient.DELETE_RQ)) { // Supprimer un fragment de HDFS
                DeleteFile(request.substring(HdfsClient.DELETE_RQ.length()));
            }
            else { // non reconnu
                System.out.println("Requete inconnue: " + request);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Créé le fragment correspondant à ce fichier et ouvre le FRW en écriture dessus
     * 
     * @param fileName
     */
    private void SetFile(String fileName) {
        // Accède au fichier du fragment
        Path pathToFragment = pathToServerDir.resolve(fileName);
        File fragment = pathToFragment.toFile();
        fragment.getParentFile().mkdir(); // Créer le répertoire si il n'existe pas
        try {
            fragment.createNewFile(); // Créer le fichier si il n'existe pas
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Ouvre le fichier en écriture
        frw.setFname(pathToFragment.toString());
        frw.open(AccessMode.WRITE);
    }

    /**
     * Supprime le fragment correspondant à ce fichier
     * 
     * @param fileName
     */
    private void DeleteFile(String fileName) {
        // Accède au fichier du fragment
        Path pathToFragment = pathToServerDir.resolve(fileName);
        File fragment = pathToFragment.toFile();
        
        // Supprime le fichier
        fragment.delete();
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            pathToServerDir = Paths.get(args[1]);
            Start (Integer.parseInt(args[0]));
        } else {
            usage();
        }
    }
}

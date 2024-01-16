package io;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import config.Project;
import hdfs.HdfsClient;

/**
 * Implémentation de l'interface NetworkReaderWriter pour gérer la communication
 * sur un réseau.
 */
public class NetworkReaderWriterImpl implements NetworkReaderWriter {
    private transient Socket socket;
    private transient ServerSocket serverSocket;
    private ObjectOutputStream objectOutputStream;
    public static BlockingQueue<KV> sharedQueue;
    private int port;
    private String host;
    private List<Thread> inputReaderThreads = new ArrayList<>();

    /**
     * Constructeur des clients
     * 
     * @param port port
     */
    public NetworkReaderWriterImpl(int port) {
        try {
            this.host = InetAddress.getLocalHost().getHostName().split("\\.")[0];
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.port = port;
        sharedQueue = new LinkedBlockingQueue<>();

        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Constructeur des clients
     * @param port numéro de port
     * @param host adresse
     */
    public NetworkReaderWriterImpl(String host, int port){
        this.port = port;
        this.host = host;
    }




    /**
     * Ouvre le serveur pour les connexions entrantes.
     */
    @Override
    public void openServer() {
        // Récupère les noeuds via le fichier config
		int nbNodes = 0;
		try {
			nbNodes = Project.getConfig(HdfsClient.CONFIGNAME).size();
		} catch (FileNotFoundException e) {
			System.out.println("Fichier de configuration non trouvé: " + HdfsClient.CONFIGNAME);
			return;
		}

        // Récupère toutes les sockets
        Socket[] sockets = new Socket[nbNodes];
        try {
            for (int i = 0; i < nbNodes; i++) {
                    sockets[i] = serverSocket.accept();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Récupère les KV des sockets et les mets dans la queue
        for (int i = 0; i < nbNodes; i++) {
            InputReader ir = new InputReader (sockets[i]);
            Thread t = new Thread(ir);
            inputReaderThreads.add(t);
            t.start();
        }
    }


    /**
     * Ferme le serveur et ServerSocket.
     */
    @Override
    public void closeServer() {
        // Fermeture du serveur et du ServerSocket
        try {
            // Ferme socket et serverSocket
            if (socket != null) {
                socket.close();
            }
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Ouvre la connexion client.
     */
    @Override
    public void openClient() {
        try {
            System.out.println("openClient: socket");
            this.socket = new Socket(host, port);

            System.out.println("openClient: stream");
            OutputStream stream = socket.getOutputStream();
            this.objectOutputStream = new ObjectOutputStream(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Ferme la connexion client.
     */
    @Override
    public void closeClient() {
        // Fermer la connexion client
        try {
            System.out.println("closeClient");
            objectOutputStream.close();
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Accepte une connexion entrante et retourne un nouvel objet
     * NetworkReaderWriterImpl pour la communication.
     *
     * @return Une nouvelle instance de NetworkReaderWriterImpl pour le client
     *         accepté.
     */
    @Override
    public NetworkReaderWriter accept() {
        // Accepter une connexion entrante et retourner un nouvel objet
        // NetworkReaderWriterImpl
        System.out.println("accept: " + host + ":" + port);
        return new NetworkReaderWriterImpl(host, port);
    }

    /**
     * Lit un objet KV depuis le flux d'entrée.
     *
     * @return L'objet KV lu ou null si la fin du flux est atteinte.
     */
    @Override
    public KV read() {
        try {
            KV read = sharedQueue.take();
            if (read.k == null && read.v == null) {
                System.out.println("read: finito");
                return null;
            }
            System.out.println("read: " + read.toString());
            return read;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Écrire un objet KV dans le flux de sortie.
     *
     * @param record L'objet KV à écrire.
     */
    @Override
    public void write(KV record) {
        // Écriture d'un objet KV dans le flux de sortie
        try {
            System.out.println("write: " + record.toString());
            objectOutputStream.writeObject(record);
            //objectOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    // Mettre les KV d'un Socket dans la queue
    private class InputReader implements Runnable {
        Socket s;

        public InputReader (Socket s) {
            this.s = s;
        }

        @Override
        public void run() {
            try {
                InputStream stream = s.getInputStream();
                ObjectInputStream objectInputStream = new ObjectInputStream(stream);

                try {
                    KV kv;
                    while ((kv = (KV)objectInputStream.readObject()) != null) {
                        NetworkReaderWriterImpl.sharedQueue.add(kv);
                    }
                } catch (EOFException e) {
                    System.out.println("finito");
                    // Signale que la queue est remplie
                    NetworkReaderWriterImpl.sharedQueue.add(new KV(null, null));
                }

                objectInputStream.close();
                stream.close();
                s.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

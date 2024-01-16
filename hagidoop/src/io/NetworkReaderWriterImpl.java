package io;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import config.Project;

/**
 * Implémentation de l'interface NetworkReaderWriter pour gérer la communication
 * sur un réseau.
 */
public class NetworkReaderWriterImpl implements NetworkReaderWriter {
    private String host; // nom de la machine
    private int port; // numéro de port

    private transient Socket socket; // pour les NRW clients
    private ObjectOutputStream objectOutputStream; // pour les NRW clients
    private transient ServerSocket serverSocket; // pour le NRW principal

    public static BlockingQueue<KV> sharedQueue; // queue commune

    /**
     * Constructeur du NRW principal
     * 
     * @param port numéro de port
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
     * Constructeur des NRW des clients
     * 
     * @param host adresse
     * @param port numéro de port
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
		int nbNodes = Project.getConfig().size(); // Nombre de noeuds

        // Récupère toutes les sockets envoyées par les NRW des clients
        Socket[] sockets = new Socket[nbNodes];
        try {
            for (int i = 0; i < nbNodes; i++) {
                    sockets[i] = serverSocket.accept();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Récupère les KV de ces sockets et les mets dans la queue
        for (int i = 0; i < nbNodes; i++) {
            InputReader ir = new InputReader (sockets[i]);
            new Thread(ir).start();
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
     * Accepte une connexion entrante d'une client et retourne un nouvel NRW
     *
     * @return Une nouvelle instance de NRW pour le client accepté.
     */
    @Override
    public NetworkReaderWriter accept() {
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
            // Si KV = null<->null, signal pour dire que la queue est remplie
            if (read.k == null && read.v == null)
                read = null;
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
            objectOutputStream.writeObject(record);
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

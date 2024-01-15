package io;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implémentation de l'interface NetworkReaderWriter pour gérer la communication
 * sur un réseau.
 */
public class NetworkReaderWriterImpl implements NetworkReaderWriter {
    private Socket socket;
    private ServerSocket serverSocket;
    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;
    private BlockingQueue<KV> sharedQueue;
    private List<Thread> receiverThreads = new ArrayList<>();
    private int port;
    private String host;

    /**
     * Constructeur pour initialiser NetworkReaderWriterImpl avec un Socket et un
     * ServerSocket.
     *
     * @param socket       Le socket client.
     * @param serverSocket Le socket serveur.
     */
    public NetworkReaderWriterImpl(Socket socket) {
        this.socket = socket;
        this.sharedQueue = new LinkedBlockingQueue<>();
        try {
            this.objectInputStream = new ObjectInputStream(socket.getInputStream());
            this.objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public NetworkReaderWriterImpl(int port, String host){
        this.port = port;
        this.host = host;
        this.sharedQueue = new LinkedBlockingQueue<>();
    }

    /**
     * Ouvre le serveur pour les connexions entrantes.
     */
    @Override
    public void openServer() {
        try {
            this.serverSocket = new ServerSocket(port);
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
            Socket socket = new Socket(host, port);
            this.objectInputStream = new ObjectInputStream(socket.getInputStream());
            this.objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
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
        try {
            Socket clientSocket = serverSocket.accept();
            NetworkReaderWriterImpl newConnection = new NetworkReaderWriterImpl(clientSocket);

            // Créer un nouveau thread Receiver pour la nouvelle connexion et l'ajouter dans
            // la liste
            Receiver receiver = new Receiver(newConnection);
            Thread thread = new Thread(receiver);
            receiverThreads.add(thread);
            thread.start();

            return newConnection;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Ferme le serveur et ServerSocket.
     */
    @Override
    public void closeServer() {
        // Fermeture du serveur et du ServerSocket
        try {
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
     * Ferme la connexion client.
     */
    @Override
    public void closeClient() {
        // Fermer la connexion client
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Lit un objet KV depuis le flux d'entrée.
     *
     * @return L'objet KV lu ou null si la fin du flux est atteinte.
     */
    @Override
    public KV read() {
        // Lecture d'un objet KV depuis le flux d'entrée
        try {
            return (KV) objectInputStream.readObject();
        } catch (EOFException e) {
            // Fin de fichier, retourne null pour indiquer la fin de la lecture
            return null;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Arrêter tous les threads Receiver.
     */
    public void stopReceiverThreads() {
        for (Thread thread : receiverThreads) {
            thread.interrupt();
        }
        receiverThreads.clear();
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
            objectOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Méthode pour signaler la fin de la lecture
    public void signalEnd() {
        write(new KV(null, null));
    }

    // Méthode pour obtenir la référence de la BlockingQueue
    public BlockingQueue<KV> getQueue() {
        return sharedQueue;
    }

    // Classe interne pour le Receiver (thread)
    private class Receiver implements Runnable {
        private NetworkReaderWriterImpl connection;

        public Receiver(NetworkReaderWriterImpl connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            while (true) {
                // Lire les KV et les mettre dans la BlockingQueue
                KV kv = connection.read();
                if (kv == null || (kv.k == null && kv.v == null)) {
                    // Fin de la lecture, ajouter le marqueur de fin dans la queue
                    connection.signalEnd();  // Ajouter cet appel pour signaler explicitement la fin
                    break;
                } else {
                    sharedQueue.offer(kv);
                }
            }
        }
        
    }
}

package io;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implémentation de l'interface NetworkReaderWriter pour gérer la communication sur un réseau.
 */
public class NetworkReaderWriterImpl implements NetworkReaderWriter {
    private Socket socket;
    private ObjectInputStream objectInputStream;
    private ObjectOutputStream objectOutputStream;
    private ServerSocket serverSocket;
    private final BlockingQueue<KV> sharedQueue;

    /**
     * Constructeur pour initialiser NetworkReaderWriterImpl avec un Socket et un ServerSocket.
     *
     * @param socket       Le socket client.
     * @param serverSocket Le socket serveur.
     */
    public NetworkReaderWriterImpl(Socket socket, ServerSocket serverSocket) {
        this.socket = socket;
        this.serverSocket = serverSocket;
        this.sharedQueue = new LinkedBlockingQueue<>();
        openClient();
        openServer();
        try {
            this.objectInputStream = new ObjectInputStream(socket.getInputStream());
            this.objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Ouvre le serveur pour les connexions entrantes.
     */
    @Override
    public void openServer() {
        // Déjà dans le constructeur ?
    }

    /**
     * Ouvre la connexion client.
     */
    @Override
    public void openClient() {
        // Déjà dans le constructeur ?
    }

    /**
     * Accepte une connexion entrante et retourne un nouvel objet NetworkReaderWriterImpl pour la communication.
     *
     * @return Une nouvelle instance de NetworkReaderWriterImpl pour le client accepté.
     */
    @Override
    public NetworkReaderWriter accept() {
        try {
            Socket clientSocket = serverSocket.accept();
            return new NetworkReaderWriterImpl(clientSocket, serverSocket);
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
     * Écrit un objet KV dans le flux de sortie.
     *
     * @param record L'objet KV à écrire.
     */
    @Override
    public void write(KV record) {
        try {
            objectOutputStream.writeObject(record);
            objectOutputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Signale la fin de la lecture en écrivant un objet KV spécial dans le flux de sortie.
     */
    public void signalEnd() {
        write(new KV(null, null));
    }

    /**
     * Obtient la référence vers la BlockingQueue.
     *
     * @return L'instance de BlockingQueue.
     */
    public BlockingQueue<KV> getQueue() {
        return sharedQueue;
    }
}

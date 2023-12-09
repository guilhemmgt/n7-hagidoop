package io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import interfaces.FileReaderWriter;
import interfaces.KV;

public class InitialNodeRW implements FileReaderWriter {
    // Nom du fichier
    private String fName = "";
    // Indice de la prochaine ligne à lire
    private int index = 0;

    // Mode d'ouverture du fichier 
    private AccessMode accessMode = AccessMode.NONE;
    
    private BufferedReader reader = null;

    @Override
    public long getIndex() {
        return this.index;
    }

    @Override
    public String getFname() {
        return this.fName;
    }

    @Override
    public void setFname(String fname) {
        this.fName = fname;
    }

    @Override
    public KV read() {
        if (accessMode != AccessMode.READ) {
            System.out.println("Fichier non ouvert en lecture");
            return new KV();
        }

        try {
		    String line = reader.readLine();
            KV kv = new KV(Integer.toString(index), line);
            index++;
            return kv;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new KV();
    }

    @Override
    public void write(KV record) {
        // cf haut page 3 du sujet : on n'implémentera pas l'écriture du HDFS dans le FS
        // donc je suppose qu'on n'implémente même pas write ?
        System.out.println("Impossible d'écrire dans le noeud initial");
    }

    @Override
    public void open(AccessMode mode) {
        switch (mode) {
            case READ:   // ouverture en lecture
                try {
                    FileReader fileReader = null;
                    fileReader = new FileReader(fName);
                    reader = new BufferedReader(fileReader);
                    accessMode = mode;
                } catch (FileNotFoundException e) {
                    System.out.println("Fichier non trouvé: " + fName);
                    e.printStackTrace();
                }
                break;
            case WRITE:   // ouverture en écriture
                // même remarque que dans write
                System.out.println("Impossible d'écrire dans le noeud initial");
                accessMode = mode;
                break;
            case NONE:    // fermeture
                close();
                break;
        }
    }

    @Override
    public void close() {
        switch (accessMode) {
            case READ:   // fermeture en lecture
                try {
                    reader.close();
                    reader = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case WRITE:   // fermeture en écriture
                // même remarque que dans write
                break;
            case NONE:   // déjà fermé
                break;
        }

        index = 0;
        accessMode = AccessMode.NONE;
    }
}

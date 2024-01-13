package io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import interfaces.FileReaderWriter;
import interfaces.KV;

public class FileReaderWriteImpl implements FileReaderWriter {
    // Nom du fichier
    private String fName = "";
    // Indice de la prochaine ligne à lire
    private int index = 0;

    // Mode d'ouverture du fichier 
    private AccessMode accessMode = AccessMode.NONE;
    
    private BufferedReader reader = null;
    private BufferedWriter writer = null;

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
    public long getFsize() {
        File file = new File (fName);
        return file.length();
    }

    @Override
    public KV read() {
        if (accessMode != AccessMode.READ) {
            System.out.println("Fichier non ouvert en lecture");
            return new KV();
        }

        try {
		    String line = reader.readLine();
            KV kv = new KV(Integer.toString(index), line); // index<->ligne
            index++;
            return kv;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new KV();
    }

    @Override
    public void write(KV record) {
        if (accessMode != AccessMode.WRITE) {
            System.out.println("Fichier non ouvert en écriture");
            return;
        }

        try {
		    writer.write(record.k + KV.SEPARATOR + record.v + "\n"); // "key<->valeur\n"
            index++;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(AccessMode mode) {
        if (accessMode != AccessMode.NONE) {
            System.out.println("Déjà ouvert");
            return;
        }

        index = 0;
        
        switch (mode) {
            case READ:   // ouverture en lecture
                try {
                    FileReader fileReader = new FileReader(fName);
                    reader = new BufferedReader(fileReader);
                    accessMode = mode;
                } catch (FileNotFoundException e) {
                    System.out.println("Fichier non trouvé: " + fName);
                    e.printStackTrace();
                }
                break;
            case WRITE:   // ouverture en écriture
                try {
                    FileWriter fileWriter = new FileWriter(fName);
                    writer = new BufferedWriter(fileWriter);
                    accessMode = mode;
                } catch (FileNotFoundException e) {
                    System.out.println("Fichier non trouvé: " + fName);
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
                    accessMode = AccessMode.NONE;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case WRITE:   // fermeture en écriture
                try {
                    writer.close();
                    writer = null;
                    accessMode = AccessMode.NONE;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case NONE:   // déjà fermé
                break;
        }
    }
}

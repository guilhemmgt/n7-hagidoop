package hdfs;

import io.AccessMode;
import io.FileReaderWriteImpl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;

public class HdfsClient {
	
	private static String CONFIGNAME = "/config.txt";
	private static FileReaderWriter frw = new FileReaderWriteImpl();

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
	}
	
	/**
	 * Permet d'écrire un fichier dans HDFS. Le fichier fname est lu sur le système
	 * de fichiers local, découpé en fragments (autant que le nombre de machines) et
	 * les fragments sont envoyés pour stockage sur les différentes machines.
	 * 
	 * @param fmt : le format du fichier (FMT_TXT ou FMT_KV)
	 * @param fname : fichier lu sur le système de fichiers local
	 */
	public static void HdfsWrite(int fmt, String fname) {
		// TODO : gérer les formats

		// Récupère les noeuds via le fichier config
		List<KV> nodes = new ArrayList<KV>();
		try {
			nodes = Project.getConfig(CONFIGNAME);
		} catch (FileNotFoundException e) {
			System.out.println("Fichier non trouvé: " + fname);
			return;
		}
		int nbNodes = nodes.size();

		// Ouvre le fichier en lecture
		frw.setFname(fname);
		frw.open(AccessMode.READ);
	
		long sizePerNode = Math.ceilDiv(frw.getFsize(), nbNodes); // Bytes à écrire par noeud
		long written = 0; // Bytes écrits sur tous les noeuds
		int nodeIndex = 0; // Noeud courant

		try {
			Socket recepteur = null;
			OutputStream recepteur_out = null;
			KV line = null;
			while ((line = frw.read()).v != null) {
				// Si pas de socket ouvert, création d'un socket
				if (recepteur == null || recepteur.isClosed()) {
					KV node = nodes.get(nodeIndex); // noeud sur lequel écrire: host<->port
					recepteur = new Socket (node.k, Integer.parseInt(node.v));
					recepteur_out = recepteur.getOutputStream ();
				}

				// Envoi au noeud
				byte[] buffer = (line.k + KV.SEPARATOR + line.v + "\n").getBytes(); // "n°_de_ligne<->ligne"
				recepteur_out.write (buffer, 0, buffer.length);
				written += line.v.getBytes().length;
				
				// Noeud rempli, fermeture du socket
				if (written >= sizePerNode * (nodeIndex + 1)) {
					recepteur.close();
					nodeIndex++;
				}
			}

			// Le dernier noeud n'est vraisemblablement pas rempli, on ferme son socket ici
			if (!recepteur.isClosed()) { 
				recepteur.close(); 
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}

		frw.close();
	}

	public static void HdfsRead(String fname) {
	}

	public static void main(String[] args) {
		// appel des méthodes précédentes depuis la ligne de commande

		if (args.length == 2 && args[0].equals("read")) { // read
			HdfsRead(args[1]);
		} else if (args.length == 3 && args[0].equals("write") && (args[1].equals("txt") || args[1].equals("kv"))) { // write
			int fmt = args[1].equals("txt") ? FileReaderWriter.FMT_TXT : FileReaderWriter.FMT_KV;
			HdfsWrite(fmt, args[2]);
		} else if (args.length == 2 && args[0].equals("delete")) { // delete
			HdfsDelete(args[1]);
		} else { // non reconnu
			usage();
		}
	}
}

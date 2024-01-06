package hdfs;

import io.AccessMode;
import io.FileReaderWriteImpl;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import config.Project;
import interfaces.KV;

public class HdfsClient /*implements Runnable*/ {
	
	public static String CONFIGNAME = "/config.txt";

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

		// Instance du ReaderWriter
		FileReaderWriteImpl rw = new FileReaderWriteImpl();

		// Ouvre le fichier en lecture
		rw.setFname(fname);
		rw.open(AccessMode.READ);
		long fileSize = rw.getFsize();
	
		// Bytes à écrire par noeud
		long sizePerNode = fileSize / nbNodes;

		// Noeud actuel
		long writtenInCurrentNode = 0;
		int currentNodeIndex = 0;

		KV line;
		while ((line = rw.read()).v != null) {
			byte[] buffer = line.v.getBytes(); // texte à écrire
			KV node = nodes.get(currentNodeIndex); // noeud sur lequel écrire

			// Écriture
			try {
				Socket recepteur = new Socket (node.k, Integer.parseInt(node.v));
				OutputStream recepteur_out = recepteur.getOutputStream ();
				recepteur_out.write (buffer, 0, buffer.length);
				writtenInCurrentNode += buffer.length;
				recepteur.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			// Changement de noeud
			if (writtenInCurrentNode >= sizePerNode) {
				writtenInCurrentNode = writtenInCurrentNode - sizePerNode;	// si on a trop écrit sur le noeud, on compense sur le prochain
				currentNodeIndex++;
			}
		}

		rw.close();
	}

	public static void HdfsRead(String fname) {
	}

	public static void main(String[] args) {
		// appel des méthodes précédentes depuis la ligne de commande

		if (args.length == 2 && args[0].equals("read")) {
			HdfsRead(args[1]);
			return;
		} else if (args.length == 3 && args[0].equals("write")) {
			try {
				HdfsWrite(Integer.parseInt(args[1]), args[2]);
				return;
			} catch (NumberFormatException e) {
				e.printStackTrace();
			}
		} else if (args.length == 2 && args[0].equals("delete")) {
			HdfsDelete(args[1]);
			return;
		}

		usage();
	}

	/*@Override
	public void run() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'run'");
	}*/
}

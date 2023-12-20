package hdfs;

import io.AccessMode;
import io.FileReaderWriteImpl;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.util.List;

import config.Project;
import interfaces.KV;

public class HdfsClient implements Runnable {
	
	public static String CONFIGNAME = "/config.txt";

	private static List<KV> nodes = null;

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
	 * les fragments sont
	 * envoyés pour stockage sur les différentes machines.
	 * 
	 * @param fmt : le format du fichier (FMT_TXT ou FMT_KV)
	 * @param fname : fichier lu sur le système de fichiers local
	 */
	public static void HdfsWrite(int fmt, String fname) {
		// Squelette de code
		nodes = Project.getConfig(CONFIGNAME);

		// Savoir combien il y a de serveurs
		int nbNodes = nodes.size();

		long fileSize = Files.size(fname);

		// Limite de taille par serv
		// TO DO : savoir la taille du fichier pour savoir quand passer au serveur suivant
		int fileSizePerServer = Math.ceil( /* taille du fichier */ / nbNodes);

		int readSinceLastWrite = 0;
		int sizePerNode = -1; // à implémenter : récup dans la config le nb de noeuds

		FileReaderWriteImpl rw = new FileReaderWriteImpl();
		rw.setFname(fname);
		rw.open(AccessMode.READ);
		KV line;
		while ((line = rw.read()) != null) {
			readSinceLastWrite += line.v.getBytes().length;

			// on envoie line.v au serveur
			Socket recepteur = new Socket ("melofee", 4000);
			OutputStream recepteur_out = recepteur.getOutputStream ();
			recepteur_out.write (buffer, 0, nb_lus);

			if (readSinceLastWrite >= sizePerNode) { // attention le dernier seuil on le dépasse pas
				readSinceLastWrite = 0;
				// on change de serveur
			}
		}

		rw.close();
	}

	public static void HdfsRead(String fname) {
	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande

		if (args.length == 2 && args[0] == "read") {
			HdfsRead(args[1]);
			return;
		} else if (args.length == 3 && args[0] == "write") {
			try {
				HdfsWrite(Integer.parseInt(args[1]), args[2]);
				return;
			} catch (NumberFormatException e) {}
		} else if (args.length == 2 && args[0] == "delete") {
			HdfsDelete(args[1]);
		}

		usage();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'run'");
	}
}

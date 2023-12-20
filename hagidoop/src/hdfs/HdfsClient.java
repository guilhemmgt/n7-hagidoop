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
		// Récupère les noeuds
		List<KV> nodes = Project.getConfig(CONFIGNAME);
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
		int nodeIndex = 0;

		KV line;
		while ((line = rw.read()) != null) {
			writtenInCurrentNode += line.v.getBytes().length;
			byte[] buffer = line.v.getBytes();
			KV node = nodes.get(nodeIndex);

			try {
				Socket recepteur = new Socket (node.k, Integer.parseInt(node.v));
				OutputStream recepteur_out = recepteur.getOutputStream ();
				recepteur_out.write (buffer, 0, buffer.length);
				recepteur.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			if (writtenInCurrentNode >= sizePerNode) {
				writtenInCurrentNode = writtenInCurrentNode - sizePerNode;	// si on a trop écrit sur le noeud, on compense sur le prochain
				nodeIndex++;
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

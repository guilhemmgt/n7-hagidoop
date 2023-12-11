package hdfs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import io.AccessMode;
import io.InitialNodeRW;
import interfaces.KV;

public class HdfsClient {
	
	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	public static void HdfsDelete(String fname) {
	}
	
	public static void HdfsWrite(int fmt, String fname) {
		// Squelette de code

		int readSinceLastWrite = 0;
		int sizePerNode = -1; // à implémenter : récup dans la config le nb de noeuds

		InitialNodeRW rw = new InitialNodeRW();
		rw.setFname(fname);
		rw.open(AccessMode.READ);

		KV line;
		while ((line = rw.read()) != null) {
			readSinceLastWrite += line.v.getBytes().length;

			if (readSinceLastWrite >= sizePerNode) {
				readSinceLastWrite = 0;
				// écriture sur le hdfs server en ssh
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
}

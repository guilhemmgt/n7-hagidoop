package daemon;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import config.Project;
import hdfs.HdfsClient;
import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import io.FileReaderWriteImpl;
import io.NetworkReaderWriterImpl;

public class JobLauncher {

	public static NetworkReaderWriterImpl nrwMain;
	public static List<Thread> threads = new ArrayList<Thread>();

	public static void startJob(MapReduce mr, int format, String fname) {
		// Récupère les noeuds via le fichier config
		List<KV> nodes = new ArrayList<KV>();
		try {
			nodes = Project.getConfig(HdfsClient.CONFIGNAME);
		} catch (FileNotFoundException e) {
			System.out.println("Fichier de configuration non trouvé: " + HdfsClient.CONFIGNAME);
			return;
		}

		// Initialise le FileReaderWriter
		FileReaderWriter frw = new FileReaderWriteImpl(FileReaderWriter.FMT_KV);
		frw.setFname(fname);

		// Initialise le NetworkReaderWriter
		try {
			String hostName = InetAddress.getLocalHost().getHostName().split("\\.")[0]; // addr.getHostName() renvoie vador.enseeiht.fr, on ne
																// souhaite récupérer que vador
			nrwMain = new NetworkReaderWriterImpl(4500); /* SI ÇA MARCHE PAS, REMPLACER hostName PAR LE NOM DE LA MACHINE EN DUR */
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		// Lance les map
		System.out.println("lance les maps");
		for (KV node : nodes) {
			System.out.println("map noeud " + node.k + ":" + node.v);
			try {
				Worker s = (Worker) Naming.lookup("//" + node.k + ":" + (Integer.parseInt(node.v)+1) + "/worker");;

				WorkerThread workerThread = new WorkerThread();
				workerThread.init(mr, frw, nrwMain, s);

				Thread thread = new Thread(workerThread);
				threads.add(thread);
				thread.start();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		nrwMain.openServer();

		mr.reduce(nrwMain, frw);

		nrwMain.closeServer();
	}
}

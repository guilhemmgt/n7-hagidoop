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
	public static List<Thread> threads;

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
		InetAddress addr;
		try {
			addr = InetAddress.getLocalHost();
			String hostName = addr.getHostName().split(".")[0]; // addr.getHostName() renvoie vador.enseeiht.fr, on ne
																// souhaite récupérer que vador
			nrwMain = new NetworkReaderWriterImpl(4000, hostName);
			nrwMain.openServer();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		// Lance les map
		for (KV node : nodes) {
			try {
				Worker s = (Worker) Naming.lookup("//" + node.k + ":" + node.v + "/worker");

				Thread t1 = new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							s.runMap(mr, frw, nrwMain);
						} catch (RemoteException e) {
							e.printStackTrace();
						}
					}
				});
				threads.add(t1);
				t1.start();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Attendre que tous les maps soient terminés
		for(Thread thr : threads){
			try {
				thr.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// récupérer les KV envoyés au reduce
		System.out.println(NetworkReaderWriterImpl.sharedQueue.toArray());

		// Ferme le NetworkReaderWriter
		nrwMain.closeServer();
	}
}

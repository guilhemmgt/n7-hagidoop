package daemon;

import java.rmi.Naming;
import java.util.List;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.MapReduce;
import io.AccessMode;
import io.FileReaderWriteImpl;
import io.NetworkReaderWriterImpl;

public class JobLauncher {
	public static NetworkReaderWriterImpl nrwMain; // NRW principal: il ne servira qu'à accept et ne fera pas transiter de données lui même

	public static void startJob(MapReduce mr, int format, String fname) {
		List<KV> nodes = Project.getConfig(); // Noeuds

		// Initialise le FRW
		FileReaderWriter frw = new FileReaderWriteImpl(FileReaderWriter.FMT_KV);
		frw.setFname(fname);

		// Initialise le NRW
		nrwMain = new NetworkReaderWriterImpl(4500); // /!\ port codé en dur

		// Lance les map sur des threads
		for (KV node : nodes) {
			try {
				// Récupère le Worker (RMI)
				Worker s = (Worker) Naming.lookup("//" + node.k + ":" + (Integer.parseInt(node.v)+1) + "/worker");;

				// Lance un thread lançant le runMap du Worker
				WorkerThread workerThread = new WorkerThread();
				workerThread.init(mr, frw, nrwMain, s);
				new Thread(workerThread).start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Ouvre le FRW en écriture
		frw.close();
		frw.open(AccessMode.WRITE);

		// Ouvre le NRW pour permettre la lecture
		nrwMain.openServer();

		// Reduce
		mr.reduce(nrwMain, frw);

		// Fermeture des RW
		nrwMain.closeServer();
		frw.close();
	}
}

package daemon;

import java.io.FileNotFoundException;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.List;

import config.Project;
import hdfs.HdfsClient;
import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.MapReduce;
import io.FileReaderWriteImpl;

public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) {
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

		// TODO Initialise le NetworkReaderWriter

		// Lance les map
		for (KV node : nodes) {
			try {
				Worker s = (Worker) Naming.lookup("//" + node.k + ":" + node.v + "/worker");
				s.runMap(mr, frw, null);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

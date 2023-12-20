package config;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

import interfaces.KV;

public class Project {
	public static String PATH = "../../.."; // dossier hagidoop

	
	/**
	 * Methode pour recuperer un fichier config.txt et renvoyer sous la forme d'une liste de paire de KV
	 *
	 * @param configName : le nom du fichier config present dans src/config/ que l'on souhaite utiliser
	 * @return kvList : la liste des kv de config (sous la forme adresseMachine, numero de port)
	 */
	public static List<KV> getConfig(String configName) throws FileNotFoundException {
		File file = new File(PATH + "/config/" + configName);

		List<String> list = new ArrayList<String>();

		// KV(adresseMachine, numero de port)
		// exemple : localhost 4000
		List<KV> kvResult = new ArrayList<KV>();

		if (file.exists()) {
			try {
				list = Files.readAllLines(file.toPath(), Charset.defaultCharset());
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} else {
			throw new FileNotFoundException();
		}

		for (String line : list) {
			String[] res = line.split(" ");
			KV kv = new KV(res[0], res[1]);
			kvResult.add(kv);
		}

		return kvResult;

	}
}

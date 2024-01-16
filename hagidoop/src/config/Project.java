package config;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.*;

import interfaces.KV;

public class Project {
	public static final String CONFIGNAME = "/config.txt"; // Nom du fichier de configuration

	public static String PATH = ".."; // dossier hagidoop

	/**
	 * Methode pour recuperer un fichier config.txt present dans le dossier config
	 * et renvoyer sous la forme d'une liste de paire de KV
	 *
	 * @param configName : le nom du fichier config present dans src/config/ que
	 *                   l'on souhaite utiliser
	 * @return kvList : la liste des kv de config (sous la forme adresseMachine,
	 *         numero de port)
	 */
	public static List<KV> getConfig() {
		File file = new File(PATH + "/config/" + CONFIGNAME);

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
			System.out.println("Fichier de configuration non trouvé: " + Project.CONFIGNAME);
		}

		for (String line : list) {
			String[] res = line.split(" ");
			KV kv = new KV(res[0], res[1]);
			kvResult.add(kv);
		}

		return kvResult;

	}
}

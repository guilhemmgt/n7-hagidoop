package daemon;

import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;
import io.AccessMode;

public class WorkerImpl extends UnicastRemoteObject implements Worker {
    private static int port;
    private static Path pathToServerDir;

    protected WorkerImpl() throws RemoteException {
        super();
        System.out.println("Worker créé. Port: " + port + ", répertoire: " + pathToServerDir);
    }

    @Override
    public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        // Ouvre le FRW en lecture, le NRW en écriture
        String fileRealName = Paths.get(reader.getFname()).getFileName().toString(); // le nom du fichier
        reader.setFname(pathToServerDir.resolve(fileRealName).toString());
        reader.open(AccessMode.READ);
        writer.openClient();

        // Map
        m.map(reader, writer);

        // Ferme les RW
        reader.close();
        writer.closeClient();
    }

    public static void main(String[] args) {
        port = Integer.parseInt(args[0]);
        pathToServerDir = Paths.get(args[1]);

        try {
            // RMI
            LocateRegistry.createRegistry(port);
            Naming.bind("//" + InetAddress.getLocalHost().getHostName().split("\\.")[0] + ":" + port + "/worker", new WorkerImpl());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }   
}

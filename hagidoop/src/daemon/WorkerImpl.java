package daemon;

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
        System.out.println("Serveur créé");
    }

    @Override
    public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
        String fileRealName = Paths.get(reader.getFname()).getFileName().toString(); // Le nom du fichier
        reader.setFname(pathToServerDir.resolve(fileRealName).toString());

        reader.open(AccessMode.READ);
        writer.openClient();

        m.map(reader, writer);

        reader.close();
        writer.closeClient();
    }

    public static void main(String[] args) {
        port = Integer.parseInt(args[0]);
        pathToServerDir = Paths.get(args[1]);

        try {
            LocateRegistry.createRegistry(port);
            Naming.bind("//melofee:" + port + "/worker", new WorkerImpl());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }   
}

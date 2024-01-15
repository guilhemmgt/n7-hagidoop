package daemon;

import java.rmi.RemoteException;

import interfaces.FileReaderWriter;
import interfaces.Map;
import interfaces.NetworkReaderWriter;

public class WorkerThread implements Runnable {

    private Map map;
    private FileReaderWriter reader;
    private NetworkReaderWriter writer;
    private Worker worker;
    

    public void init(Map map, FileReaderWriter reader, NetworkReaderWriter writer, Worker worker){
        this.map = map;
        this.reader = reader;
        this.writer = writer;
        this.worker = worker;
    }

    @Override
    public void run() {
        try {
            System.out.println("running map");
            worker.runMap(map, reader, writer);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }
    
}

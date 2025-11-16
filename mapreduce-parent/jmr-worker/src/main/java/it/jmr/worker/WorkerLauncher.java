package it.jmr.worker;

import java.util.concurrent.ExecutorService;

import it.jmr.common.discovery.DiscoverableService;

public class WorkerLauncher {
    public static final ExecutorService executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: WorkerNode <worker-id> <port>");
            System.out.println("Esempio: WorkerNode worker1 8081");
            return;
        }

        final String workerId = args[0];
        final int port = Integer.parseInt(args[1]);

        // Avvio ricerca pubblicazione servizi
        final DiscoverableService service = new DiscoverableService(workerId, "_jmr._tcp.local.", port);
        service.start();

        // Avvio server worker
        final String jarStorageDir = "./worker-data/" + workerId + "/jars/";
        final String jobStorageDir = "./worker-data/" + workerId + "/jobs/";
        final WorkerServer worker = new WorkerServer(workerId, port, jarStorageDir, jobStorageDir);
        worker.start();
        worker.blockUntilShutdown();
    }

}

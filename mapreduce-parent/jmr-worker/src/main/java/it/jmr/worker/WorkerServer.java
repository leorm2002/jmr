package it.jmr.worker;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.protobuf.services.HealthStatusManager;
import it.jmr.common.JMRConstants;
import it.jmr.common.jarservice.JarServiceImpl;
import it.jmr.common.jarservice.JobServiceImpl;
import it.jmr.common.utils.JMRLog;
import it.jmr.common.utils.Pair;
import it.jmr.worker.models.WorkerContext;
import it.jmr.worker.storage.InMemoryIntermediateStorage;

public class WorkerServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServer.class);
    private final int port;
    private final Server server;
    final String storageDir;
    final WorkerContext ctx;
    private HealthStatusManager healthStatusManager;

    public WorkerServer(String workerId, int port, String jarStorageDir, String jobStorageDir) {
        this.port = port;
        this.storageDir = jarStorageDir;
        this.ctx = new WorkerContext(new InMemoryIntermediateStorage(), workerId);
        this.healthStatusManager = new HealthStatusManager();

        // Crea directory di storage
        new File(jarStorageDir).mkdirs();

        // Crea directory di storage per i job
        new File(jobStorageDir).mkdirs();

        this.server = ServerBuilder.forPort(port)//
                .addService(new WorkerServiceImpl(ctx))//
                .addService(new JarServiceImpl(jarStorageDir, ctx.jarStorage))//
                .addService(new JobServiceImpl(jobStorageDir, ctx.jobStorage))//
                .addService(healthStatusManager.getHealthService()) // Aggiungi health service
                .maxInboundMessageSize(JMRConstants.MAX_INBOUND_MESSAGE_SIZE).build();

        // Imposta lo stato di salute
        healthStatusManager.setStatus(JMRConstants.HEALTH_SERVICE_NAME, HealthCheckResponse.ServingStatus.SERVING);
    }

    public void start() throws IOException {
        server.start();
        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║            jMR   Worker Node           ║");
        System.out.println("╚════════════════════════════════════════╝");
        System.out.println("Worker ID:    " + ctx.workerId);
        System.out.println("Porta:        " + port);
        System.out.println("Storage:      " + new File(storageDir).getAbsolutePath());
        System.out.println("\nWorker pronto per ricevere task...\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("\nShutdown del worker...");
            WorkerServer.this.stop();
        }));
    }

    public void stop() {
        if (server != null) {
            JMRLog.debug(LOGGER, "Shutting down health service...");
            healthStatusManager.enterTerminalState();
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @SuppressWarnings("unchecked")
    private <KEY extends Serializable, VALUE extends Serializable> List<Pair<KEY, VALUE>> fetchIntermediateDataFromWorker(String workerId,
            String host, int port, String taskId, int partitionId) throws Exception {

        // Se è questo worker, leggi direttamente dal disco
        if (workerId.equals(this.ctx.workerId)) {
            String filePath = storageDir + "/map_" + taskId + "_part_" + partitionId + ".dat";
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
                return (List<Pair<KEY, VALUE>>) ois.readObject();
            }
        }

        // Altrimenti, fetch via gRPC
        // TODO: implement gRPC client to fetch from remote worker
        throw new UnsupportedOperationException("Remote fetch not yet implemented");
    }

    @SuppressWarnings("unchecked")
    <T> T deserialize(byte[] data) throws Exception {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (T) ois.readObject();
        }
    }

}
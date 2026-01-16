package it.jmr.tests;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.jmr.common.jarservice.JarServiceImpl;
import it.jmr.common.jarservice.JobServiceImpl;
import it.jmr.common.jarservice.ResourceUploadedCallback;
import it.jmr.grpc.JarServiceGrpc;
import it.jmr.grpc.JobServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages test servers for integration testing.
 * Provides methods to start/stop gRPC servers and create clients.
 */
public class TestServerManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestServerManager.class);
    private static final AtomicInteger PORT_COUNTER = new AtomicInteger(50000);

    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final List<Path> tempDirs = new ArrayList<>();

    /**
     * Information about a started test server
     */
    public record TestServerInfo(
            Server server,
            int port,
            String jarStorageDir,
            String jobStorageDir,
            ConcurrentHashMap<String, String> jarStorage,
            ConcurrentHashMap<String, String> jobStorage,
            TestResourceCallback callback
    ) {}

    /**
     * Callback implementation that tracks uploaded resources for verification
     */
    public static class TestResourceCallback implements ResourceUploadedCallback {
        private final List<String> uploadedJars = new ArrayList<>();
        private final List<String> uploadedJobs = new ArrayList<>();

        @Override
        public synchronized void onJarUploaded(String jarId, String jarPath) {
            LOGGER.info("Test callback: JAR uploaded - ID: {}, Path: {}", jarId, jarPath);
            uploadedJars.add(jarId);
        }

        @Override
        public synchronized void onJobUploaded(String jobId, String jobPath) {
            LOGGER.info("Test callback: Job uploaded - ID: {}, Path: {}", jobId, jobPath);
            uploadedJobs.add(jobId);
        }

        public synchronized List<String> getUploadedJars() {
            return new ArrayList<>(uploadedJars);
        }

        public synchronized List<String> getUploadedJobs() {
            return new ArrayList<>(uploadedJobs);
        }

        public synchronized void clear() {
            uploadedJars.clear();
            uploadedJobs.clear();
        }
    }

    /**
     * Starts a new test server with JarService and JobService
     */
    public TestServerInfo startServer() throws IOException {
        int port = PORT_COUNTER.getAndIncrement();

        Path tempDir = Files.createTempDirectory("jmr-test-");
        tempDirs.add(tempDir);

        String jarStorageDir = tempDir.resolve("jars").toString();
        String jobStorageDir = tempDir.resolve("jobs").toString();

        new File(jarStorageDir).mkdirs();
        new File(jobStorageDir).mkdirs();

        ConcurrentHashMap<String, String> jarStorage = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, String> jobStorage = new ConcurrentHashMap<>();
        TestResourceCallback callback = new TestResourceCallback();

        Server server = ServerBuilder.forPort(port)
                .addService(new JarServiceImpl(jarStorageDir, jarStorage, callback))
                .addService(new JobServiceImpl(jobStorageDir, jobStorage, callback))
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build()
                .start();

        servers.add(server);
        LOGGER.info("Started test server on port {}", port);

        return new TestServerInfo(server, port, jarStorageDir, jobStorageDir, jarStorage, jobStorage, callback);
    }

    /**
     * Creates a JarService async stub for the given server
     */
    public JarServiceGrpc.JarServiceStub createJarServiceStub(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        channels.add(channel);
        return JarServiceGrpc.newStub(channel);
    }

    /**
     * Creates a JobService async stub for the given server
     */
    public JobServiceGrpc.JobServiceStub createJobServiceStub(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();
        channels.add(channel);
        return JobServiceGrpc.newStub(channel);
    }

    /**
     * Creates a temporary JAR file for testing
     */
    public Path createTestJarFile(String content) throws IOException {
        Path tempDir = Files.createTempDirectory("jmr-test-jar-");
        tempDirs.add(tempDir);
        Path jarFile = tempDir.resolve("test.jar");
        Files.write(jarFile, content.getBytes());
        return jarFile;
    }

    /**
     * Creates a temporary file with the given content
     */
    public Path createTempFile(String prefix, String suffix, byte[] content) throws IOException {
        Path tempDir = Files.createTempDirectory("jmr-test-file-");
        tempDirs.add(tempDir);
        Path file = tempDir.resolve(prefix + suffix);
        Files.write(file, content);
        return file;
    }

    @Override
    public void close() {
        // Shutdown channels
        for (ManagedChannel channel : channels) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
        channels.clear();

        // Shutdown servers
        for (Server server : servers) {
            server.shutdown();
            try {
                server.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }
        servers.clear();

        // Cleanup temp directories
        for (Path tempDir : tempDirs) {
            try {
                Files.walk(tempDir)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                LOGGER.warn("Failed to cleanup temp directory: {}", tempDir, e);
            }
        }
        tempDirs.clear();

        LOGGER.info("Test server manager closed");
    }
}

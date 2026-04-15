package it.jmr.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.exceptions.JMRException;

public class InteractiveClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(InteractiveClient.class);
    private static final long MONITOR_POLL_INTERVAL_MS = 2_000L;

    private static void handleCommand(final String line, final MapReduceClient client) {
        final String[] parts = line.trim().split("\\s+");
        if (parts.length == 0 || parts[0].isEmpty()) {
            return;
        }

        final String command = parts[0].toLowerCase();

        try {
            switch (command) {
            case "submit":
                if (parts.length < 3) {
                    LOGGER.error("Usage: submit <jar-path> <serialized-job-path>");
                    break;
                }

                final Path jarPath = Path.of(parts[1]);
                final Path jobPath = Path.of(parts[2]);
                final String jobId = client.submit(jarPath, jobPath);
                LOGGER.info("Job submitted: {}", jobId);
                break;

            case "status":
                if (parts.length < 2) {
                    LOGGER.error("Usage: status <job-id>");
                    break;
                }

                logJobProgress(parts[1], client.getJobProgress(parts[1]));
                break;

            case "list":
                client.listJobs();
                break;

            case "cancel":
                if (parts.length < 2) {
                    LOGGER.error("Usage: cancel <job-id>");
                    break;
                }

                client.cancelJob(parts[1]);
                LOGGER.info("Cancellation requested for job {}", parts[1]);
                break;

            case "result":
                if (parts.length < 3) {
                    LOGGER.error("Usage: result <job-id> <output-path>");
                    break;
                }

                final Path outputPath = Path.of(parts[2]);
                Files.write(outputPath, client.getJobResult(parts[1]));
                LOGGER.info("Serialized result for job {} written to {}", parts[1], outputPath.toAbsolutePath());
                break;

            case "monitor":
                if (parts.length < 2) {
                    LOGGER.error("Usage: monitor <job-id>");
                    break;
                }

                monitorJob(parts[1], client);
                break;

            case "help":
                printHelp();
                break;

            default:
                LOGGER.error("Unknown command: {}", command);
                printHelp();
                break;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Command interrupted", e);
        } catch (IOException e) {
            LOGGER.error("I/O error executing command: {}", e.getMessage(), e);
        } catch (JMRException e) {
            LOGGER.error("Error executing command: {}", e.getMessage(), e);
        }
    }

    private static void monitorJob(final String jobId, final MapReduceClient client) throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            final MapReduceClient.JobProgressSnapshot progress = client.getJobProgress(jobId);
            logJobProgress(jobId, progress);

            if (progress.isTerminal()) {
                return;
            }

            Thread.sleep(MONITOR_POLL_INTERVAL_MS);
        }
    }

    private static void logJobProgress(final String jobId, final MapReduceClient.JobProgressSnapshot progress) {
        if (!progress.found()) {
            LOGGER.info("Job {} status: {}", jobId, progress.status());
            return;
        }

        LOGGER.info("Job {} status: {} | MAP {}% | REDUCE {}%", jobId, progress.status(), progress.mapProgress(), progress.reduceProgress());
    }

    private static void printHelp() {
        LOGGER.info("\nAvailable commands:\n" + "  submit <jar-path> <serialized-job-path>       - Submits a job\n"
                + "  status <job-id>                               - Gets the status of a job\n"
                + "  list                                          - Lists all jobs\n"
                + "  cancel <job-id>                               - Cancels a running or pending job\n"
                + "  result <job-id> <output-path>                 - Downloads the serialized job result\n"
                + "  monitor <job-id>                              - Monitors a job in real time\n"
                + "  help                                          - Shows this help\n"
                + "  exit | quit                                   - Exits the client\n");
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            LOGGER.error("Usage: java -jar mapreduce-client.jar <master-host> <master-port>");
            LOGGER.error("After startup, use 'help' for the list of interactive commands.");
            return;
        }

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);

        try (final Scanner scanner = new Scanner(System.in)) {
            try (final MapReduceClient client = new MapReduceClient(host, port)) {
                LOGGER.info("Connected to {}:{}. Type 'help' for commands or 'exit' to quit.", host, port);

                while (true) {
                    System.out.print("mapreduce-client> ");
                    final String line = scanner.nextLine();

                    if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                        break;
                    }

                    handleCommand(line, client);
                }

            } catch (Exception e) {
                LOGGER.error("Critical client error: {}", e.getMessage(), e);
            }
        }
        LOGGER.info("Client terminated.");
    }
}

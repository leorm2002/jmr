package it.jmr.client;

import java.nio.file.Path;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.exceptions.JMRException;

public class InteractiveClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(InteractiveClient.class);

    private static void handleCommand(String line, MapReduceClient client) {
        final String[] parts = line.trim().split("\\s+");
        if (parts.length == 0 || parts[0].isEmpty()) {
            return;
        }

        final String command = parts[0].toLowerCase();

        try {
            switch (command) {
            case "submit":
                if (parts.length < 3) {
                    LOGGER.error("Usage: submit <jar-path> <main-class> [job-args...]");
                    break;
                }
                final Path jarPath = Path.of(parts[1]);
                final String mainClass = parts[2];
                final String jarId = client.uploadJar(jarPath);
                final String jobId = client.submitJob(jarId, mainClass);
                LOGGER.info("\n✓ Job submitted: " + jobId);
                break;

            case "status":
                if (parts.length < 2) {
                    LOGGER.error("Usage: status <job-id>");
                    break;
                }
                client.getJobStatus(parts[1]);
                break;

            case "list":
                client.listJobs();
                break;
            case "help":
                printHelp();
                break;

            default:
                LOGGER.error("Unknown command: " + command);
                printHelp();
                break;
            }
        } catch (JMRException e) {
            LOGGER.error("Error executing command: " + e.getMessage(), e);
        }
    }

    private static void printHelp() {
        LOGGER.info("\nAvailable commands:\n" + "  submit <jar-path> <main-class> [job-args...]  - Submits a job\n"
                + "  status <job-id>                               - Gets the status of a job\n"
                + "  list                                          - Lists all jobs\n"
                + "  monitor <job-id>                              - Monitors a job in real time\n"
                + "  help                                          - Shows this help\n"
                + "  exit | quit                                   - Exits the client\n");
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            LOGGER.error("Usage: java -jar mapreduce-client.jar <master-host> <master-port>");
            LOGGER.error("After startup, use 'help' for the list of interactive commands.");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        try (Scanner scanner = new Scanner(System.in)) {
            try (MapReduceClient client = new MapReduceClient(host, port)) {
                LOGGER.info("Connected to {}:{}. Type 'help' for commands or 'exit' to quit.", host, port);

                while (true) {
                    System.out.print("mapreduce-client> ");
                    String line = scanner.nextLine();

                    if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                        break;
                    }

                    handleCommand(line, client);
                }

            } catch (Exception e) {
                LOGGER.error("Critical client error: " + e.getMessage(), e);
            }
        }
        LOGGER.info("Client terminated.");
    }
}
package com.example;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import it.jmr.client.JMRClient;
import it.jmr.client.Job;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.Pair;
import it.jmr.grpcdataprovider.localgrpc.LocalGrpcDataProvider;

public final class MyFailingJob {
    private static final long STATUS_POLL_INTERVAL_MS = 5_000L;

    private MyFailingJob() {
    }

    public static void main(final String[] args) throws InterruptedException, JMRException {
        if (args.length < 5 || args.length > 6) {
            System.err.println(
                    "Usage: MyFailingJob <serialized-books-data-path> <jar-path> <host> <port> <failure-phase: map|reduce> [data-provider-host]");
            System.exit(1);
        }

        final String booksPath = args[0];
        final Path jarPath = Path.of(args[1]);
        final String host = args[2];
        final int port = Integer.parseInt(args[3]);
        final FailurePhase failurePhase = FailurePhase.fromArgument(args[4]);
        final String dataProviderHost = args.length >= 6 ? args[5] : "localhost";

        final List<Path> books = collectSerializedBooks(Path.of(booksPath));
        final LocalGrpcDataProvider<String> dataProviderServer = new LocalGrpcDataProvider<>(books);
        dataProviderServer.setServerHost(dataProviderHost);

        final JobConfiguration<String, Integer, Integer> job = createFailingJob(dataProviderServer, failurePhase);

        try {
            final JMRClient jmrClient = new JMRClient(host, port);
            try {
                jmrClient.submit(jarPath, job);

                while (true) {
                    final it.jmr.client.MapReduceClient.JobProgressSnapshot progress = jmrClient.getJobProgress();
                    final String status = progress.status();
                    System.out.printf("Job status: %s | MAP %d%% | REDUCE %d%%%n", status, progress.mapProgress(), progress.reduceProgress());

                    if ("FAILED".equals(status)) {
                        System.out.println("Failing job reached FAILED as expected.");
                        return;
                    }

                    if ("COMPLETED".equals(status) || "CANCELLED".equals(status)) {
                        throw new JMRException("Failing job finished with unexpected status: " + status);
                    }

                    Thread.sleep(STATUS_POLL_INTERVAL_MS);
                }
            } finally {
                closeClient(jmrClient);
            }
        } finally {
            dataProviderServer.close();
        }
    }

    private static List<Path> collectSerializedBooks(final Path booksFolder) {
        final List<Path> books = new ArrayList<>();
        try (var paths = java.nio.file.Files.list(booksFolder)) {
            paths.filter(java.nio.file.Files::isRegularFile).filter(path -> path.toString().endsWith(".ser")).forEach(books::add);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list book files in folder: " + booksFolder, e);
        }

        if (books.isEmpty()) {
            throw new IllegalArgumentException("No serialized .ser files found in folder: " + booksFolder);
        }
        return books;
    }

    private static JobConfiguration<String, Integer, Integer> createFailingJob(final LocalGrpcDataProvider<String> dataProviderServer,
            final FailurePhase failurePhase) {
        return Job.builder().readFrom(dataProviderServer).map(line -> {
            if (failurePhase == FailurePhase.MAP) {
                throw new IllegalStateException("Intentional MAP failure for integration testing");
            }

            final List<Pair<String, Integer>> results = new ArrayList<>();
            for (final String word : line.split("\\W+")) {
                if (!word.isEmpty()) {
                    results.add(Pair.of(word.toLowerCase(), 1));
                }
            }
            return results;
        }).reduce((key, values) -> {
            if (failurePhase == FailurePhase.REDUCE) {
                throw new IllegalStateException("Intentional REDUCE failure for integration testing");
            }

            final int sum = values.stream().mapToInt(Integer::intValue).sum();
            return Pair.of(key, sum);
        });
    }

    private static void closeClient(final JMRClient jmrClient) throws InterruptedException, JMRException {
        try {
            jmrClient.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (Exception e) {
            throw new JMRException("Failed to close JMR client", e);
        }
    }

    private enum FailurePhase {
        MAP,
        REDUCE;

        static FailurePhase fromArgument(final String argument) {
            return switch (argument.toLowerCase()) {
            case "map" -> MAP;
            case "reduce" -> REDUCE;
            default -> throw new IllegalArgumentException("Unsupported failure phase: " + argument + ". Expected map or reduce.");
            };
        }
    }
}

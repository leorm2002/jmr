package com.example;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import it.jmr.client.JMRClient;
import it.jmr.client.Job;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.Pair;
import it.jmr.grpcdataprovider.localgrpc.LocalGrpcDataProvider;

public class MyWcJob {
    private static final long STATUS_POLL_INTERVAL_MS = 5_000L;

    public static void main(String[] args) throws InterruptedException, JMRException {

        if (args.length < 4 || args.length > 6) {
            System.err.println("Usage: MyWcJob <serialized-books-data-path> <jar-path> <host> <port> [data-provider-host] [result-output-path]");

            System.out.println("Received parameters:");
            for (int i = 0; i < args.length; i++) {
                System.out.printf("args[%d]: %s%n", i, args[i]);
            }
            System.exit(1);
        }

        final String booksPath = args[0];
        final Path jarPath = Path.of(args[1]);
        final String host = args[2];
        final int port = Integer.parseInt(args[3]);
        final String dataProviderHost = args.length >= 5 ? args[4] : "localhost";
        final Path resultOutputPath = args.length >= 6 ? Path.of(args[5]) : null;

        // Creo il mio grpc data provider server
        final List<Path> books = new ArrayList<>();
        final Path booksFolder = Path.of(booksPath);
        try (var paths = java.nio.file.Files.list(booksFolder)) {
            paths.filter(java.nio.file.Files::isRegularFile).filter(path -> path.toString().endsWith(".ser")).forEach(books::add);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list book files in folder: " + booksFolder, e);
        }
        if (books.isEmpty()) {
            throw new IllegalArgumentException("No serialized .ser files found in folder: " + booksFolder);
        }

        final LocalGrpcDataProvider<String> dataProviderServer = new LocalGrpcDataProvider<>(books);
        dataProviderServer.setServerHost(dataProviderHost);

        // Configuro e lancio il job di MapReduce
        final JobConfiguration<String, Integer, Integer> job = Job.builder()//
                .readFrom(dataProviderServer)//
                .map(line -> {
                    final List<Pair<String, Integer>> results = new java.util.ArrayList<>();
                    // Mapper: suddivide la linea in parole e emette (parola, 1)
                    for (String word : line.split("\\W+")) {
                        if (!word.isEmpty()) {
                            results.add(new Pair<>(word.toLowerCase(), 1));
                        }
                    }
                    return results;
                }).reduce((entr, values) -> {
                    // Reducer: somma i conteggi per ogni parola
                    final int sum = values.stream().mapToInt(Integer::intValue).sum();
                    return new Pair<>(entr, sum);
                });

        try {
            final JMRClient jmrClient = new it.jmr.client.JMRClient(host, port);
            try {
                // Invio il mio job al cluster
                jmrClient.submit(jarPath, job);

                String finalStatus = null;
                while (true) {
                    final it.jmr.client.MapReduceClient.JobProgressSnapshot progress = jmrClient.getJobProgress();
                    final String status = progress.status();
                    System.out.printf("Job status: %s | MAP %d%% | REDUCE %d%%%n", status, progress.mapProgress(), progress.reduceProgress());

                    if ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
                        finalStatus = status;
                        break;
                    }

                    Thread.sleep(STATUS_POLL_INTERVAL_MS);
                }

                if (!"COMPLETED".equals(finalStatus)) {
                    throw new JMRException("Word count job finished with status: " + finalStatus);
                }

                if (resultOutputPath != null) {
                    final byte[] serializedResult = jmrClient.getJobResult();
                    try {
                        Files.createDirectories(resultOutputPath.toAbsolutePath().getParent());
                        Files.write(resultOutputPath, serializedResult);
                    } catch (java.io.IOException e) {
                        throw new JMRException("Failed to write job result to " + resultOutputPath.toAbsolutePath(), e);
                    }
                    System.out.println("Serialized result written to " + resultOutputPath.toAbsolutePath());
                }
            } finally {
                closeClient(jmrClient);
            }
        } catch (InterruptedException e) {
            throw e;
        } finally {
            dataProviderServer.close();
        }
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

}

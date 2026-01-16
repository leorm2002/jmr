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

public class MyWcJob {

    public static void main(String[] args) throws InterruptedException, JMRException {

        if (args.length != 4) {
            System.err.println("Usage: MyWcJob <books-data-path> <jar-path> <host> <port>");

            System.out.println("Received parameters:");
            for (int i = 0; i < args.length; i++) {
                System.out.printf("args[%d]: %s%n", i, args[i]);
            }
            System.exit(1);
        }

        final String booksPath = args[0];
        final String jarPath = args[1];
        final String host = args[2];
        final int port = Integer.parseInt(args[3]);

        // Creo il mio grpc data provider server
        List<Path> books = new ArrayList<>();
        Path booksFolder = Path.of(booksPath);
        try (var paths = java.nio.file.Files.list(booksFolder)) {
            paths.filter(java.nio.file.Files::isRegularFile).forEach(books::add);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list book files in folder: " + booksFolder, e);
        }
        final LocalGrpcDataProvider<String> dataProviderServer = new LocalGrpcDataProvider<>(books);

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

        // TODO: check esistenza all'avvio
        final JMRClient jmrClient = new it.jmr.client.JMRClient(host, port);

        // Invio il mio job al cluster
        jmrClient.submit(jarPath, job);

        while (true) {
            final String status = jmrClient.getJobStatus();
            System.out.println("Job status: " + status);
            Thread.sleep(10000); // Attendi 10 secondi prima di controllare di nuovo

            if ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
                break;
            }
        }
    }

}

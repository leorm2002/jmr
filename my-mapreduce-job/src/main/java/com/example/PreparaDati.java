package com.example;

import it.jmr.common.exceptions.JMRException;
import it.jmr.grpcdataprovider.Container;
import it.jmr.grpcdataprovider.DataProviderUtils;

public class PreparaDati {
    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: PreparaDati <booksInputPath> <booksOutputPath>");
            System.exit(1);
        }

        final String booksInputPath = args[0];
        final String booksOutputPath = args[1];
        java.io.File dir = new java.io.File(booksInputPath);
        java.io.File[] files = dir.listFiles((d, name) -> name.endsWith(".txt"));

        if (files != null) {
            for (java.io.File file : files) {
                final Container<String> book = loadBook(file.getAbsolutePath());
                // Puoi aggiungere qui la logica per processare ogni libro
                System.out.println("Caricato libro da: " + file.getName() + ", righe: " + book.data.size());

                String outName = booksOutputPath + java.io.File.separator + file.getName().replace(".txt", ".ser");
                try {
                    DataProviderUtils.serialize(java.nio.file.Paths.get(outName), book.data);
                    System.out.println("Serializzato libro in: " + outName);
                } catch (JMRException e) {
                    System.err.println("Errore serializzando il libro in " + outName + ": " + e.getMessage());
                }
            }
        } else {
            System.err.println("Nessun file trovato in " + booksInputPath);
        }

    }

    public static Container<String> loadBook(String bookPath) {
        // Carica il libro da file di testo, ogni riga è un elemento
        try {
            java.util.List<String> lines = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(bookPath));
            return new Container<>(lines);
        } catch (Exception e) {
            throw new RuntimeException("Errore caricando il libro da " + bookPath, e);
        }
    }

}

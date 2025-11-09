package it.jmr.client;
import java.util.Scanner;
// Assumendo che il resto della classe MapReduceClient esista...
// public class MapReduceClient { ... }

public class InteractiveClient {

    // Spostiamo la logica di gestione dei comandi in un metodo separato
    private static void handleCommand(String line, MapReduceClient client) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 0 || parts[0].isEmpty()) {
            return; // Riga vuota
        }

        String command = parts[0].toLowerCase();

        try {
            switch (command) {
                case "submit":
                    if (parts.length < 3) {
                        System.err.println("Uso: submit <jar-path> <main-class> [job-args...]");
                        break;
                    }
                    String jarPath = parts[1];
                    String mainClass = parts[2];
                    String jarId = client.uploadJar(jarPath);
                    String jobId = client.submitJob(jarId, mainClass); // Modifica qui se devi passare 'parts'
                    System.out.println("\n✓ Job sottomesso: " + jobId);
                    break;

                case "status":
                    if (parts.length < 2) {
                        System.err.println("Uso: status <job-id>");
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
                    System.err.println("Comando sconosciuto: " + command);
                    printHelp();
                    break;
            }
        } catch (Exception e) {
            // Gestisce errori *del comando* senza terminare il client
            System.err.println("Errore durante l'esecuzione del comando: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Un metodo helper per stampare l'aiuto
    private static void printHelp() {
        System.out.println();
        System.out.println("Comandi disponibili:");
        System.out.println("  submit <jar-path> <main-class> [job-args...]  - Sottomette un job");
        System.out.println("  status <job-id>                               - Ottiene lo stato di un job");
        System.out.println("  list                                          - Lista tutti i job");
        System.out.println("  monitor <job-id>                              - Monitora un job in tempo reale");
        System.out.println("  help                                          - Mostra questo aiuto");
        System.out.println("  exit | quit                                   - Esce dal client");
        System.out.println();
    }

    // Il nuovo metodo main
    public static void main(String[] args) {
        // 1. Controlla gli argomenti di avvio (solo host e porta)
        if (args.length != 2) {
            System.out.println("Uso: java -jar mapreduce-client.jar <master-host> <master-port>");
            System.out.println();
            System.out.println("Dopo l'avvio, usare 'help' per la lista dei comandi interattivi.");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        MapReduceClient client = null;
        Scanner scanner = new Scanner(System.in);

        try {
            // 2. Inizializza il client UNA SOLA VOLTA
            client = new MapReduceClient(host, port);
            System.out.println("Connesso a " + host + ":" + port + ". Digita 'help' per i comandi o 'exit' per uscire.");

            // 3. Avvia il ciclo REPL (Read-Eval-Print Loop)
            while (true) {
                System.out.print("mapreduce-client> ");
                String line = scanner.nextLine();

                // Controlla il comando di uscita
                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                    break; // Esce dal ciclo while
                }

                // Gestisce il comando
                handleCommand(line, client);
            }

        } catch (Exception e) {
            // Errore critico (es. connessione fallita all'inizio)
            System.err.println("Errore critico del client: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 4. Esegue lo shutdown UNA SOLA VOLTA all'uscita
            System.out.println("Disconnessione in corso...");
            if (client != null) {
                try {
                    client.shutdown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            scanner.close();
            System.out.println("Client terminato.");
        }
    }
}
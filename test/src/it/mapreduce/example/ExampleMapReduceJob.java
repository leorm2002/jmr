// ExampleMapReduceJob.java
package it.mapreduce.example;

/**
 * Esempio di job MapReduce che può essere compilato in un JAR e inviato al master
 */
public class ExampleMapReduceJob {
    
    public static class Container{
        String a;

        public String getA() {
            return a;
        }

        public void setA(String a) {
            this.a = a;
        }
        
    }
    public static void main(String[] args) {
        System.out.println("=== Avvio Example MapReduce Job ===");
        
        if (args.length > 0) {
            System.out.println("Argomenti ricevuti:");
            for (int i = 0; i < args.length; i++) {
                System.out.println("  arg[" + i + "] = " + args[i]);
            }
        }
        
        // Simulazione di un job MapReduce
        System.out.println("\n[FASE MAP]");
        String[] data = {"hello world", "hello mapreduce", "world of java"};
        
        for (String line : data) {
            String[] words = line.split(" ");
            for (String word : words) {
                System.out.println("  MAP: (" + word + ", 1)");
            }
        }
        
        Container aa = new Container();
        aa.setA("!");
        System.out.println("\n[FASE SHUFFLE]");
        System.out.println("  Raggruppamento per chiave...");
        
        System.out.println("\n[FASE REDUCE]");
        System.out.println("  REDUCE: (hello, 2)");
        System.out.println("  REDUCE: (world, 2)");
        System.out.println("  REDUCE: (mapreduce, 1)");
        System.out.println("  REDUCE: (of, 1)");
        System.out.println("  REDUCE: (java, 1)");
        
        System.out.println("\n=== Job completato con successo ===");
    }
}

/**
 * Script bash per compilare ed eseguire l'esempio:
 * 
 * # Compila il job di esempio
 * javac -d build ExampleMapReduceJob.java
 * jar cvf example-job.jar -C build .
 * 
 * # Avvia il master (in un terminale)
 * java -cp mapreduce.jar it.mapreduce.master.MapReduceMaster 9999
 * 
 * # Invia il job (in un altro terminale)
 * java -cp mapreduce.jar it.mapreduce.client.MapReduceClient \
 *   localhost 9999 example-job.jar it.mapreduce.example.ExampleMapReduceJob input.txt output.txt
 */
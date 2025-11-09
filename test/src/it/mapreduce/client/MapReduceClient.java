// MapReduceClient.java
package it.mapreduce.client;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;

public class MapReduceClient {
    private String masterHost;
    private int masterPort;
    
    public MapReduceClient(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }
    
    /**
     * Invia il JAR al master e avvia il job MapReduce
     */
    public void submitJob(String jarPath, String mainClass, String[] args) {
        try (Socket socket = new Socket(masterHost, masterPort);
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
             DataInputStream dis = new DataInputStream(socket.getInputStream())) {
            
            // Invia il comando di submit
            dos.writeUTF("SUBMIT_JOB");
            
            // Invia il nome della classe principale
            dos.writeUTF(mainClass);
            
            // Invia gli argomenti
            dos.writeInt(args.length);
            for (String arg : args) {
                dos.writeUTF(arg);
            }
            
            // Leggi il file JAR
            File jarFile = new File(jarPath);
            byte[] jarBytes = Files.readAllBytes(jarFile.toPath());
            
            // Invia la dimensione del JAR
            dos.writeLong(jarBytes.length);
            System.out.println("Invio JAR di " + jarBytes.length + " bytes...");
            
            // Invia il contenuto del JAR
            dos.write(jarBytes);
            dos.flush();
            
            // Ricevi conferma
            String response = dis.readUTF();
            System.out.println("Risposta dal master: " + response);
            
            if (response.equals("JOB_ACCEPTED")) {
                String jobId = dis.readUTF();
                System.out.println("Job ID: " + jobId);
                
                // Monitora lo stato del job
                monitorJobStatus(jobId);
            }
            
        } catch (IOException e) {
            System.err.println("Errore durante il submit del job: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void monitorJobStatus(String jobId) {
        // TODO: Implementare il monitoraggio dello stato del job
        System.out.println("Monitoraggio job " + jobId + " in corso...");
    }
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: MapReduceClient <master-host> <master-port> <jar-path> <main-class> [job-args...]");
            return;
        }
        
        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        String jarPath = args[2];
        String mainClass = args[3];
        String[] jobArgs = new String[args.length - 4];
        System.arraycopy(args, 4, jobArgs, 0, jobArgs.length);
        
        MapReduceClient client = new MapReduceClient(masterHost, masterPort);
        client.submitJob(jarPath, mainClass, jobArgs);
    }
}
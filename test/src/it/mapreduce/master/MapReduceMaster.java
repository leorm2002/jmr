package it.mapreduce.master;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduceMaster {
    private int port;
    private ExecutorService executorService;
    private ConcurrentHashMap<String, JobInfo> jobs;
    private String jarStorageDir = "./jars";
    
    public MapReduceMaster(int port) {
        this.port = port;
        this.executorService = Executors.newCachedThreadPool();
        this.jobs = new ConcurrentHashMap<>();
        
        // Crea la directory per i JAR se non esiste
        new File(jarStorageDir).mkdirs();
    }
    
    public void start() {
        System.out.println("Master MapReduce avviato sulla porta " + port);
        
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Nuova connessione da: " + clientSocket.getInetAddress());
                
                executorService.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.err.println("Errore nel server master: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void handleClient(Socket clientSocket) {
        try (DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream())) {
            
            String command = dis.readUTF();
            
            if (command.equals("SUBMIT_JOB")) {
                handleJobSubmission(dis, dos);
            } else {
                dos.writeUTF("UNKNOWN_COMMAND");
            }
            
        } catch (IOException e) {
            System.err.println("Errore nella gestione del client: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void handleJobSubmission(DataInputStream dis, DataOutputStream dos) throws IOException {
        // Leggi la classe principale
        String mainClass = dis.readUTF();
        
        // Leggi gli argomenti
        int argsCount = dis.readInt();
        String[] args = new String[argsCount];
        for (int i = 0; i < argsCount; i++) {
            args[i] = dis.readUTF();
        }
        
        // Leggi la dimensione del JAR
        long jarSize = dis.readLong();
        System.out.println("Ricezione JAR di " + jarSize + " bytes...");
        
        // Genera ID univoco per il job
        String jobId = UUID.randomUUID().toString();
        
        // Salva il JAR su disco
        String jarPath = jarStorageDir + "/" + jobId + ".jar";
        try (FileOutputStream fos = new FileOutputStream(jarPath)) {
            byte[] buffer = new byte[8192];
            long remaining = jarSize;
            
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                int bytesRead = dis.read(buffer, 0, toRead);
                if (bytesRead == -1) break;
                
                fos.write(buffer, 0, bytesRead);
                remaining -= bytesRead;
            }
        }
        
        System.out.println("JAR salvato: " + jarPath);
        
        // Crea info del job
        JobInfo jobInfo = new JobInfo(jobId, mainClass, args, jarPath);
        jobs.put(jobId, jobInfo);
        
        // Invia conferma al client
        dos.writeUTF("JOB_ACCEPTED");
        dos.writeUTF(jobId);
        dos.flush();
        
        // Avvia l'esecuzione del job in modo asincrono
        executorService.submit(() -> executeJob(jobInfo));
    }
    
    private void executeJob(JobInfo jobInfo) {
        System.out.println("Avvio esecuzione job: " + jobInfo.getJobId());
        jobInfo.setStatus(JobStatus.RUNNING);
        
        try {
            // Carica il JAR usando un ClassLoader dinamico
            JobClassLoader classLoader = new JobClassLoader(jobInfo.getJarPath());
            
            // Carica la classe principale
            Class<?> mainClass = classLoader.loadClass(jobInfo.getMainClass());
            
            // Ottieni il metodo main
            java.lang.reflect.Method mainMethod = mainClass.getMethod("main", String[].class);
            
            // Esegui il metodo main
            mainMethod.invoke(null, (Object) jobInfo.getArgs());
            
            jobInfo.setStatus(JobStatus.COMPLETED);
            System.out.println("Job completato: " + jobInfo.getJobId());
            
        } catch (Exception e) {
            jobInfo.setStatus(JobStatus.FAILED);
            jobInfo.setErrorMessage(e.getMessage());
            System.err.println("Errore nell'esecuzione del job " + jobInfo.getJobId() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9999;
        MapReduceMaster master = new MapReduceMaster(port);
        master.start();
    }
}

class JobInfo {
    private String jobId;
    private String mainClass;
    private String[] args;
    private String jarPath;
    private JobStatus status;
    private String errorMessage;
    
    public JobInfo(String jobId, String mainClass, String[] args, String jarPath) {
        this.jobId = jobId;
        this.mainClass = mainClass;
        this.args = args;
        this.jarPath = jarPath;
        this.status = JobStatus.PENDING;
    }
    
    // Getters e setters
    public String getJobId() { return jobId; }
    public String getMainClass() { return mainClass; }
    public String[] getArgs() { return args; }
    public String getJarPath() { return jarPath; }
    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}

enum JobStatus {
    PENDING, RUNNING, COMPLETED, FAILED
}
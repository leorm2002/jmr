import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import it.jmr.common.providers.DataProviderClient;
import it.jmr.grpcdataprovider.Container;
import it.jmr.grpcdataprovider.localgrpc.LocalGrpcDataProvider;
import it.jmr.grpcdataprovider.remotegrpc.RemoteGrpcDataProvider;

public class RemoteGrpcExample {

    static class Person implements Serializable {
        private static final long serialVersionUID = 1L;
        String name;
        int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{name='" + name + "', age=" + age + "}";
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("\n\n=== Esempio con server esterno (RemoteGrpcDataProvider) ===\n");
        exampleWithRemoteServer();
    }

    /**
     * Scenario 2: Connessione a server esterno con RemoteGrpcDataProvider
     */
    private static void exampleWithRemoteServer() throws Exception {
        // SETUP: Prima avviamo un server esterno per simulare
        Path dataFile = prepareTestData();
        LocalGrpcDataProvider<Person> externalServer = new LocalGrpcDataProvider<>(dataFile, "192.168.1.100");
        externalServer.init();
        int externalPort = externalServer.getServerPort();

        System.out.println("Server esterno simulato su: 192.168.1.100:" + externalPort);
        System.out.println("(In realtà gira su localhost:" + externalPort + " per il test)\n");

        // ===== CASO D'USO REALE =====
        // Ci connettiamo a un server gRPC esterno esistente
        // Il server potrebbe essere in un altro datacenter, cloud, ecc.

        RemoteGrpcDataProvider<Person> remoteProvider = new RemoteGrpcDataProvider<>("localhost", externalPort); // In produzione: IP esterno
        remoteProvider.init();

        // Otteniamo un client preconfigurato
        DataProviderClient<Person> client = remoteProvider.getClient();

        // Possiamo serializzare questo client e inviarlo ai worker
        DataProviderClient<Person> serializedClient = serializeAndDeserialize(client);
        serializedClient.init();

        // I worker usano il client per accedere ai dati dal server esterno
        System.out.println("Worker remoto accede al server esterno:");
        System.out.println("Size: " + serializedClient.size());
        System.out.println("Chunk 1: " + serializedClient.fetchChunk(0, 2));
        System.out.println("Chunk 2: " + serializedClient.fetchChunk(2, 2));

        // Cleanup
        serializedClient.close();
        remoteProvider.close();
        externalServer.close();
        dataFile.toFile().delete();
    }

    @SuppressWarnings("unchecked")
    private static <D extends Serializable> DataProviderClient<D> serializeAndDeserialize(DataProviderClient<D> client)
            throws IOException, ClassNotFoundException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(client);
        }

        System.out.println("Client serializzato: " + baos.size() + " bytes\n");

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (DataProviderClient<D>) ois.readObject();
        }
    }

    private static Path prepareTestData() throws IOException {
        List<Person> people = Arrays.asList(new Person("Alice", 30), new Person("Bob", 25), new Person("Charlie", 35), new Person("Diana", 28));

        Container<Person> container = new Container<>(people);

        Path tempFile = java.nio.file.Files.createTempFile("testdata", ".dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(java.nio.file.Files.newOutputStream(tempFile))) {
            oos.writeObject(container);
        }

        return tempFile;
    }
}

/*
 * OUTPUT ATTESO: === Esempio con server locale (FSDataProvider) ===
 * 
 * FSDataProvider worker started on localhost:50051 Server locale avviato su:
 * localhost:50051 Size: 4 Chunk: [Person{name='Alice', age=30},
 * Person{name='Bob', age=25}] FSDataProvider worker stopped
 * 
 * 
 * === Esempio con server esterno (RemoteGrpcDataProvider) ===
 * 
 * FSDataProvider worker started on 192.168.1.100:50052 Server esterno simulato
 * su: 192.168.1.100:50052 (In realtà gira su localhost:50052 per il test)
 * 
 * RemoteGrpcDataProvider configured to connect to localhost:50052 Client
 * serializzato: 85 bytes
 * 
 * RemoteGrpcDataProviderClient connected to localhost:50052 Worker remoto
 * accede al server esterno: Size: 4 Chunk 1: [Person{name='Alice', age=30},
 * Person{name='Bob', age=25}] Chunk 2: [Person{name='Charlie', age=35},
 * Person{name='Diana', age=28}] RemoteGrpcDataProviderClient disconnected from
 * localhost:50052 RemoteGrpcDataProvider closed FSDataProvider worker stopped
 */
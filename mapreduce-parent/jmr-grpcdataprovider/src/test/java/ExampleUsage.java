
import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import it.jmr.common.providers.DataProviderClient;
import it.jmr.grpcdataprovider.Container;
import it.jmr.grpcdataprovider.localgrpc.LocalGrpcDataProvider;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class ExampleUsage {

    // Esempio di classe Serializable per i dati
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
        // 1. Prepara i dati di test e salvali su file
        Path dataFile = prepareTestData();

        // 2. LATO MASTER: Crea il provider server e inizializza il worker
        LocalGrpcDataProvider<Person> provider = new LocalGrpcDataProvider<>(dataFile);
        provider.init();

        System.out.println("Server avviato su: " + provider.getServerHost() + ":" + provider.getServerPort());

        // 3. MASTER crea un client preconfigurato
        DataProviderClient<Person> client = provider.getClient();
        System.out.println("Client creato: " + client);

        // 4. SIMULA INVIO DEL CLIENT A UN WORKER REMOTO
        // In un sistema reale, questo client verrebbe serializzato e inviato via rete
        DataProviderClient<Person> remoteClient = serializeAndDeserialize(client);

        // 5. LATO WORKER REMOTO: Inizializza il client deserializzato
        remoteClient.init();

        // 6. Il worker remoto usa il client per accedere ai dati
        // Tutti i metodi di fetch sono solo sul client!
        System.out.println("\n=== Worker remoto accede ai dati ===");
        System.out.println("Size totale: " + remoteClient.size());

        // Fetch primo chunk
        List<Person> chunk1 = remoteClient.fetchChunk(0, 3);
        System.out.println("Chunk 1 (offset=0, limit=3): " + chunk1);

        // Fetch secondo chunk
        List<Person> chunk2 = remoteClient.fetchChunk(3, 3);
        System.out.println("Chunk 2 (offset=3, limit=3): " + chunk2);

        // 7. Chiudi risorse
        remoteClient.close();
        provider.close();

        // Cleanup
        dataFile.toFile().delete();
    }

    /**
     * Simula la serializzazione e deserializzazione del client come avverrebbe
     * inviandolo via rete a un worker remoto.
     */
    @SuppressWarnings("unchecked")
    private static <D extends Serializable> DataProviderClient<D> serializeAndDeserialize(DataProviderClient<D> client)
            throws IOException, ClassNotFoundException {

        // Serializza
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(client);
        }

        System.out.println("\nClient serializzato: " + baos.size() + " bytes");

        // Deserializza (simula ricezione su worker remoto)
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (DataProviderClient<D>) ois.readObject();
        }
    }

    private static Path prepareTestData() throws IOException {
        List<Person> people = Arrays.asList(new Person("Alice", 30), new Person("Bob", 25), new Person("Charlie", 35), new Person("Diana", 28),
                new Person("Eve", 32), new Person("Frank", 27));

        Container<Person> container = new Container<>(people);

        Path tempFile = java.nio.file.Files.createTempFile("testdata", ".dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(java.nio.file.Files.newOutputStream(tempFile))) {
            oos.writeObject(container);
        }

        return tempFile;
    }
}

/*
 * OUTPUT ATTESO: FSDataProvider worker started on localhost:50051 Server
 * avviato su: localhost:50051 Client creato:
 * FSDataProviderClient{host='localhost', port=50051}
 * 
 * Client serializzato: 85 bytes
 * 
 * === Worker remoto accede ai dati === Size totale: 6 Chunk 1 (offset=0,
 * limit=3): [Person{name='Alice', age=30}, Person{name='Bob', age=25},
 * Person{name='Charlie', age=35}] Chunk 2 (offset=3, limit=3):
 * [Person{name='Diana', age=28}, Person{name='Eve', age=32},
 * Person{name='Frank', age=27}] FSDataProvider worker stopped
 */
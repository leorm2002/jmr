package it.jmr.common.jarservice;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.JMRLog;

/**
 * ClassLoader personalizzato per caricare job da JAR con supporto per
 * deserializzazione
 */
public class JobClassLoader extends URLClassLoader {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(JobClassLoader.class);
    private final String jarPath;
    private final String filePath;

    /**
     * Crea un JobClassLoader con percorso JAR e file
     * 
     * @param jarPath  Percorso del file JAR contenente le classi del job
     * @param filePath Percorso del file serializzato (può essere null se non usato)
     * @throws Exception Se il JAR non esiste o non può essere caricato
     */
    public JobClassLoader(String jarPath, String filePath) throws Exception {
        super(new URL[] { new File(jarPath).toURI().toURL() }, JobClassLoader.class.getClassLoader());

        this.jarPath = jarPath;
        this.filePath = filePath;

        // Verifica che il JAR esista
        if (!new File(jarPath).exists()) {
            throw new FileNotFoundException("JAR non trovato: " + jarPath);
        }

        // Verifica che il file esista (se specificato)
        if (filePath != null && !new File(filePath).exists()) {
            throw new FileNotFoundException("File non trovato: " + filePath);
        }
    }

    /**
     * Costruttore semplificato - solo JAR
     */
    public JobClassLoader(String jarPath) throws Exception {
        this(jarPath, null);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        // Prima prova a caricare dal JAR
        try {
            return super.loadClass(name);
        } catch (ClassNotFoundException e) {
            // Se non trovata nel JAR, delega al parent classloader
            return findClass(name);
        }
    }

    /**
     * Carica una classe JobConfiguration dal JAR
     * 
     * @param name Nome completo della classe (es. "com.example.MyJob")
     * @return La classe caricata
     * @throws ClassNotFoundException Se la classe non esiste o non estende
     *                                JobConfiguration
     */
    @SuppressWarnings("unchecked")
    public <D extends Serializable, V extends Serializable, O extends Serializable> Class<? extends JobConfiguration<D, V, O>> loadJob(String name)
            throws ClassNotFoundException {
        Class<?> clazz;
        try {

            clazz = loadClass(name);
        } catch (ReflectiveOperationException e) {
            throw new ClassNotFoundException("Errore durante il caricamento della classe: " + name, e);
        }

        if (!JobConfiguration.class.isAssignableFrom(clazz)) {
            throw new ClassNotFoundException(name + " non estende JobConfiguration");
        }

        return (Class<? extends JobConfiguration<D, V, O>>) clazz;
    }

    /**
     * Carica e istanzia un job dal JAR
     * 
     * @param name Nome completo della classe
     * @return Istanza del job
     * @throws Exception Se il caricamento o l'istanziazione fallisce
     */
    public JobConfiguration<?, ?, ?> loadAndInstantiateJob(String name) throws Exception {
        Class<? extends JobConfiguration<?, ?, ?>> jobClass = loadJob(name);
        return jobClass.getDeclaredConstructor().newInstance();
    }

    /**
     * Deserializza un oggetto dal file usando questo ClassLoader
     * 
     * @param <T> Tipo dell'oggetto deserializzato
     * @return L'oggetto deserializzato
     * @throws Exception Se il file non è specificato o la deserializzazione
     *                   fallisce
     */
    @SuppressWarnings("unchecked")
    public <T> T deserializeFromFile() throws Exception {
        if (filePath == null) {
            throw new IllegalStateException("Nessun file specificato nel costruttore");
        }
        T deserialize;
        try {

            byte[] data = Files.readAllBytes(Paths.get(filePath));
            deserialize = deserialize(data);

        } catch (InvalidObjectException e) {
            JMRLog.error(LOGGER, "Errore di deserializzazione: classe non trovata {}", e);

            // Stack trace completo
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            JMRLog.error(LOGGER, "Stack trace:\n{}", sw.toString());

            // Cerca ClassNotFoundException nella catena
            Throwable current = e;
            while (current != null) {
                JMRLog.error(LOGGER, "❌ {}", current.getMessage());
                current = current.getCause();
            }

            throw e;
        } catch (Exception e) {
            JMRLog.error(LOGGER, "Errore durante la deserializzazione {}", e);
            throw new RuntimeException(e);
        }
        return deserialize;
    }

    /**
     * Deserializza un oggetto da bytes usando questo ClassLoader
     * 
     * @param data Bytes dell'oggetto serializzato
     * @param <T>  Tipo dell'oggetto deserializzato
     * @return L'oggetto deserializzato
     * @throws Exception Se la deserializzazione fallisce
     */
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] data) throws Exception {
        Thread currentThread = Thread.currentThread();
        ClassLoader originalClassLoader = currentThread.getContextClassLoader();

        try {
            // Imposta questo ClassLoader come context classloader
            currentThread.setContextClassLoader(this);

            try (JobObjectInputStream ois = new JobObjectInputStream(new ByteArrayInputStream(data), this)) {
                return (T) ois.readObject();
            }
        } finally {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }

    /**
     * Serializza un oggetto in bytes
     * 
     * @param obj Oggetto da serializzare
     * @return Bytes dell'oggetto serializzato
     * @throws IOException Se la serializzazione fallisce
     */
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }

    /**
     * Serializza un oggetto in un file
     * 
     * @param obj        Oggetto da serializzare
     * @param outputPath Percorso del file di output
     * @throws IOException Se la serializzazione fallisce
     */
    public static void serializeToFile(Object obj, String outputPath) throws IOException {
        byte[] data = serialize(obj);
        Files.write(Paths.get(outputPath), data);
    }

    public String getJarPath() {
        return jarPath;
    }

    public String getFilePath() {
        return filePath;
    }

    /**
     * ObjectInputStream personalizzato che usa il JobClassLoader per risolvere le
     * classi
     */
    private static class JobObjectInputStream extends ObjectInputStream {
        private final ClassLoader classLoader;

        public JobObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            try {
                // Usa il JobClassLoader per caricare la classe
                return Class.forName(desc.getName(), false, classLoader);
            } catch (ClassNotFoundException e) {
                // Fallback al comportamento standard
                return super.resolveClass(desc);
            }
        }
    }
}

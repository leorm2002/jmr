package it.jmr.common.jarservice;

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.exceptions.JMRException;
import it.jmr.common.utils.JMRLog;

/**
 * Custom ClassLoader for loading jobs from JARs with deserialization support
 */
public class JobClassLoader extends URLClassLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobClassLoader.class);
    private final Path filePath;

    /**
     * Creates a JobClassLoader with JAR and file path
     * 
     * @param jarPath  Path to the JAR file containing the job classes
     * @param filePath Path to the serialized file (can be null if not used)
     * @throws JMRException If the JAR does not exist or cannot be loaded
     */
    public JobClassLoader(Path jarPath, Path filePath) throws JMRException {
        super(getJarUrl(jarPath), JobClassLoader.class.getClassLoader());

        this.filePath = filePath;

        // Verify that the JAR exists
        if (!jarPath.toFile().exists()) {
            throw new JMRException("JAR not found: " + jarPath);
        }

        // Verify that the file exists (if specified)
        if (filePath != null && !filePath.toFile().exists()) {
            throw new JMRException("File not found: " + filePath);
        }
    }

    private static URL[] getJarUrl(Path jarPath) throws JMRException {
        try {
            return new URL[] { new File(jarPath.toString()).toURI().toURL() };
        } catch (java.net.MalformedURLException e) {
            throw new JMRException("Invalid JAR path", e);
        }
    }

    /**
     * Simplified constructor - only JAR
     */
    public JobClassLoader(Path jarPath) throws JMRException {
        this(jarPath, null);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        // First try to load from the JAR
        try {
            return super.loadClass(name);
        } catch (ClassNotFoundException e) {
            // If not found in the JAR, delegate to the parent classloader
            return findClass(name);
        }
    }

    /**
     * Deserializes an object from the file using this ClassLoader
     * 
     * @param <T> Type of the deserialized object
     * @return The deserialized object
     * @throws JMRException If the file is not specified or deserialization fails
     */
    public <T> T deserializeFromFile() throws JMRException {
        if (filePath == null) {
            throw new IllegalStateException("No file specified in the constructor");
        }
        LOGGER.debug("Deserializing from file: {}", filePath);
        T deserialize;
        try {

            byte[] data = Files.readAllBytes(filePath);
            deserialize = deserialize(data);

        } catch (InvalidObjectException e) {
            JMRLog.error(LOGGER, "Deserialization error: class not found {}", e);

            // Full stack trace
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            JMRLog.error(LOGGER, "Stack trace:\n{}", sw.toString());

            // Search for ClassNotFoundException in the chain
            Throwable current = e;
            while (current != null) {
                JMRLog.error(LOGGER, "❌ {}", current.getMessage());
                current = current.getCause();
            }

            throw new JMRException("Deserialization error", e);
        } catch (Exception e) {
            JMRLog.error(LOGGER, "Error during deserialization {}", e);
            throw new JMRException("Error during deserialization", e);
        }
        return deserialize;
    }

    /**
     * Deserializes an object from bytes using this ClassLoader
     * 
     * @param data Bytes of the serialized object
     * @param <T>  Type of the deserialized object
     * @return The deserialized object
     * @throws JMRException If deserialization fails
     */
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] data) throws JMRException {
        Thread currentThread = Thread.currentThread();
        ClassLoader originalClassLoader = currentThread.getContextClassLoader();

        try {
            // Set this ClassLoader as the context classloader
            currentThread.setContextClassLoader(this);

            try (JobObjectInputStream ois = new JobObjectInputStream(new ByteArrayInputStream(data), this)) {
                return (T) ois.readObject();
            }
        } catch (Exception e) {
            throw new JMRException("Error during deserialization", e);
        } finally {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }

    /**
     * Serializes an object into bytes
     * 
     * @param obj Object to serialize
     * @return Bytes of the serialized object
     * @throws IOException If serialization fails
     */
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }

    /**
     * Custom ObjectInputStream that uses the JobClassLoader to resolve classes
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
                // Use the JobClassLoader to load the class
                return Class.forName(desc.getName(), false, classLoader);
            } catch (ClassNotFoundException e) {
                // Fallback to the default behavior
                return super.resolveClass(desc);
            }
        }
    }
}

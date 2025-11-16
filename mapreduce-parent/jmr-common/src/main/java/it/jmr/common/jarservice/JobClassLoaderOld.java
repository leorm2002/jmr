package it.jmr.common.jarservice;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.Pair;

public class JobClassLoaderOld extends URLClassLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobClassLoaderOld.class);

    public JobClassLoaderOld(String jarPath) throws Exception {
        super(new URL[] { new File(jarPath).toURI().toURL() },
                JobClassLoaderOld.class.getClassLoader());
    }

    @SuppressWarnings("unchecked")
    public <D extends Serializable, V extends Serializable, O extends Serializable> Class<? extends JobConfiguration<D, V, O>> loadJob(
            String name) throws ClassNotFoundException {
        Class<?> clazz;
        try {
            clazz = findClass(name);
        } catch (ClassNotFoundException e) {
            clazz = super.loadClass(name);
        }
        if (!JobConfiguration.class.isAssignableFrom(clazz)) {
            throw new ClassNotFoundException(name + " does not extend JobConfiguration");
        }
        return (Class<? extends JobConfiguration<D, V, O>>) clazz;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);

            if (c == null) {
                // IMPORTANT: Your interfaces/base classes must be loaded by the PARENT
                // Only user implementations are loaded from the JAR
                if (name.startsWith("com.yourpackage.api.") || // Your interfaces
                        name.startsWith("java.") ||
                        name.startsWith("javax.")) {
                    // Always delegate to the parent for shared classes
                    c = getParent().loadClass(name);
                } else {
                    // For user classes, first search in the JAR
                    try {
                        c = findClass(name);
                    } catch (ClassNotFoundException e) {
                        c = getParent().loadClass(name);
                    }
                }
            }

            if (resolve) {
                resolveClass(c);
            }

            return c;
        }
    }

    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> JobConfiguration<D, V, O> loadJob(
            String jarId, String mainClass) {

        // 1. Load the job from the jar
        // 2. Retrieve data from the DataProvider
        // 3. Execute the mapping
        // 4. Save the intermediate data

        try (JobClassLoaderOld classLoader = new JobClassLoaderOld(jarId)) {
            Thread.currentThread().setContextClassLoader(classLoader);

            Class<?> rawJobClass = classLoader.loadJob(mainClass);
            @SuppressWarnings("unchecked")
            Class<JobConfiguration<D, V, O>> jobClass = (Class<JobConfiguration<D, V, O>>) rawJobClass;

            JobConfiguration<D, V, O> jobConfig = jobClass.getDeclaredConstructor().newInstance();

            return jobConfig;

        } catch (Exception e) {
            LOGGER.error("Error loading job from jar", e);
            throw new RuntimeException("Error loading job from jar: " + e.getMessage(), e);
        }
    }

}
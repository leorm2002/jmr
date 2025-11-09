package it.jmr.common.jarservice;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;

import it.jmr.common.models.JobConfiguration;
import it.jmr.common.utils.Pair;

public class JobClassLoaderOld extends URLClassLoader {
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
                // IMPORTANTE: Le tue interfacce/classi base devono essere caricate dal PARENT
                // Solo le implementazioni dell'utente vengono caricate dal JAR
                if (name.startsWith("com.tuopackage.api.") || // Le tue interfacce
                        name.startsWith("java.") ||
                        name.startsWith("javax.")) {
                    // Delega sempre al parent per le classi condivise
                    c = getParent().loadClass(name);
                } else {
                    // Per le classi dell'utente, cerca prima nel JAR
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

        // 1. Carica il job dal jar
        // 2. Recupera i dati dal DataProvider
        // 3. Esegui la mappatura
        // 4. Salva i dati intermedi

        try (JobClassLoaderOld classLoader = new JobClassLoaderOld(jarId)) {
            Thread.currentThread().setContextClassLoader(classLoader);

            Class<?> rawJobClass = classLoader.loadJob(mainClass);
            @SuppressWarnings("unchecked")
            Class<JobConfiguration<D, V, O>> jobClass = (Class<JobConfiguration<D, V, O>>) rawJobClass;

            JobConfiguration<D, V, O> jobConfig = jobClass.getDeclaredConstructor().newInstance();

            return jobConfig;

        } catch (Exception e) {
            throw new RuntimeException("Error in the loading of the job from the jar: " + e.getMessage(), e);
        }
    }

}
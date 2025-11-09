// JobClassLoader.java
package it.mapreduce.master;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * ClassLoader personalizzato per caricare dinamicamente le classi dai JAR dei job
 */
public class JobClassLoader extends URLClassLoader {
    
    public JobClassLoader(String jarPath) throws Exception {
        super(new URL[]{new File(jarPath).toURI().toURL()}, 
              JobClassLoader.class.getClassLoader());
    }
    
    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        // Prova a caricare prima dal JAR del job
        try {
            return findClass(name);
        } catch (ClassNotFoundException e) {
            // Se non trovata, delega al parent classloader
            return super.loadClass(name);
        }
    }
}
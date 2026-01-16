package it.jmr.common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;

public class JmrUtils {
    public static void sleep(int pauseTimeInMillis) {
        try {
            Thread.sleep(pauseTimeInMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static String generateJobId() {
        return "JOB-" + java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    public static String generateJarId() {
        return "JAR-" + java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    public static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    public static void deleteFolder(Path folderPath) {
        java.io.File folder = folderPath.toFile();
        if (folder.exists() && folder.isDirectory()) {
            for (java.io.File file : folder.listFiles()) {
                if (file.isDirectory()) {
                    deleteFolder(file.toPath());
                } else {
                    file.delete();
                }
            }
            folder.delete();
        }
    }

    @SuppressWarnings("unchecked")
    public static <O extends Serializable> O deserialize(byte[] data) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (O) ois.readObject();
        }
    }

    public static byte[] serializeObject(Serializable obj) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
        }
        return baos.toByteArray();
    }

}

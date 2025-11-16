package it.jmr.grpcdataprovider;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

/**
 * Utility class fot the users of the provided grpc data providers, it exposes a simple way to build the serialized file
 */
public class DataProviderUtils {

    public static <O extends Serializable> void serialize(Path p, List<O> toSerialize) throws IOException {
        Container<O> c = new Container<>(toSerialize);
        try (ObjectOutputStream oos = new ObjectOutputStream(
                java.nio.file.Files.newOutputStream(p))) {
            oos.writeObject(c);
        }
    }
}

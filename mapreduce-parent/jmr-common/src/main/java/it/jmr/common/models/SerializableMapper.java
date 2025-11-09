package it.jmr.common.models;

import java.io.Serializable;
import java.util.List;

import it.jmr.common.utils.Pair;

@FunctionalInterface
public interface SerializableMapper<D extends Serializable, V extends Serializable>
        extends Serializable {
    List<Pair<String, V>> apply(D t);
}

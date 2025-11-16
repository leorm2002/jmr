package it.jmr.common.models;

import java.io.Serializable;
import java.util.List;

import it.jmr.common.utils.Pair;

@FunctionalInterface
public interface SerializableMapper<D extends Serializable, V extends Serializable> extends Serializable {
    static final long serialVersionUID = 1L;

    List<Pair<String, V>> apply(D t);
}

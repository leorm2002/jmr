package it.jmr.common.models;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

import it.jmr.common.utils.Pair;

public interface SerializableReducer<V extends Serializable, O extends Serializable>
                extends BiFunction<String, List<V>, Pair<String, List<O>>>, Serializable {

}

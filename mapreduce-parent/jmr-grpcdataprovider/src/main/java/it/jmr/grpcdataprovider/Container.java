package it.jmr.grpcdataprovider;

import java.io.Serializable;
import java.util.List;

public class Container<D extends Serializable> implements Serializable {
    private static final long serialVersionUID = 1L;
    public final List<D> data;
    
    public Container(List<D> data) {
        this.data = data;
    }
}
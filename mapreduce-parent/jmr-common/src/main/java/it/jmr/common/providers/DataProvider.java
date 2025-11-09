package it.jmr.common.providers;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Interfaccia per un DataProvider server-side.
 * Gestisce i dati localmente e può creare client per accesso remoto.
 */
public interface DataProvider<D extends Serializable> {
    
    /**
     * Inizializza il provider e avvia il worker server.
     */
    DataProvider<D> init();
    
    
    /**
     * Crea un client preconfigurato per accesso remoto a questo provider.
     * Il client può essere serializzato e inviato ai worker remoti.
     *
     * @return un DataProviderClient configurato per connettersi a questo server.
     */
    DataProviderClient<D> getClient();
    
    /**
     * Rilascia eventuali risorse (connessioni, file handler, worker server, ecc.).
     *
     * @throws IOException se il rilascio risorse fallisce.
     */
    void close() throws IOException;
}
package it.jmr.common.providers;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interfaccia per un client remoto che accede a un DataProvider.
 * Implementazioni di questa interfaccia sono serializzabili e possono essere
 * inviate dal master ai worker remoti per accedere ai dati distribuiti.
 */
public interface DataProviderClient<D extends Serializable> extends Serializable, AutoCloseable {
    static final long serialVersionUID = 1L;

    /**
     * Inizializza la connessione al server remoto. Deve essere chiamato prima di
     * qualsiasi operazione di fetch.
     */
    void init();

    /**
     * Restituisce la dimensione totale dei dati dal server remoto.
     *
     * @return numero totale di elementi disponibili.
     * @throws IOException se si verificano problemi di connessione o accesso.
     */
    long size() throws IOException;

    /**
     * Recupera un blocco di dati dal server remoto.
     *
     * @param offset posizione iniziale (0-based).
     * @param limit  numero massimo di elementi da leggere.
     * @return lista di elementi recuperati (può essere vuota se offset >= size).
     * @throws IOException se si verificano problemi di connessione o accesso.
     */
    List<D> fetchChunk(long offset, long limit) throws IOException;

    /**
     * Chiude la connessione al server remoto.
     *
     * @throws IOException se la chiusura fallisce.
     */
    @Override
    void close() throws IOException;
}
package it.jmr.worker;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import it.jmr.common.jarservice.JobClassLoader;
import it.jmr.common.jarservice.JobClassLoaderOld;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.models.SerializableMapper;
import it.jmr.common.models.SerializableReducer;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.worker.SubmitMapTaskRequest;
import it.jmr.grpc.worker.SubmitReduceTaskRequest;
import it.jmr.worker.models.WorkerContext;

/**
 * Static class that executes the map and reduce tasks
 */
public class WorkerExecutor {

    public static <D extends Serializable, V extends Serializable, O extends Serializable> void executeReduce(SubmitReduceTaskRequest request) {

        try (JobClassLoaderOld classLoader = new JobClassLoaderOld(request.getJarId())) {
            String jarId = request.getJarId();
            JobConfiguration<D, V, O> jobConfig;
            try {
                jobConfig = new JobClassLoader(jarId, "job.ser").deserializeFromFile();
            } catch (Exception e) {
                throw new RuntimeException("Error in the loading of the job from the jar: " + e.getMessage(), e);
            }

            final SerializableReducer<V, O> reducer = jobConfig.getReducer();
            // Recupera i dati da location intermedie
            List<Pair<String, List<V>>> mappedData = Collections.emptyList();

            // Esegue la riduzione

            Set<Entry<String, List<List<V>>>> partitionedData = mappedData.parallelStream()
                    .collect(Collectors.groupingByConcurrent(Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList()))).entrySet();

            List<Pair<String, O>> reducedData = partitionedData.parallelStream()
                    .map(e -> reducer.apply(e.getKey(), e.getValue().stream().flatMap(List::stream).collect(Collectors.toList())))
                    .collect(Collectors.toList());

            // System.out.println("--- Output del job ---");
            // System.out.println(reducedData);
            // System.out.println("--- Fine output ---");

        } catch (Exception e) {
            // jobInfo.setStatus(JobStatus.FAILED);
            // jobInfo.setErrorMessage(e.getMessage());
            // jobInfo.notifyEvent("Job fallito: " + e.getMessage());
            // System.err.println("<<< Job fallito: " + jobInfo.getJobId());
            // e.printStackTrace();
        } finally {
            // Libero il posto in coda
            // jobQueue.poll();
            // Eliminro il jar
            // new File(jobInfo.getJarPath()).delete();
            // jarsPaths.remove(jobInfo.getJarPath());
            // Notifico la fine agli listeners
            // jobInfo.completeEventListeners();
        }
    }

    /**
     * Executes the map task as per the request
     */
    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> Map<String, List<V>> executeMap(
            SubmitMapTaskRequest request, WorkerContext ctx) {

        // 1. Carica il job dal jar
        // 2. Recupera i dati dal DataProvider
        // 3. Esegui la mappatura
        // 4. Salva i dati intermedi

        // 1st step: load the job configuration
        final String jarId = request.getJarId();

        final String jarPath = ctx.jarStorage.get(jarId);
        final String jobPath = ctx.jobStorage.get(request.getJobId());

        JobConfiguration<D, V, O> jobConfig;
        try {
            jobConfig = new JobClassLoader(jarPath, jobPath).deserializeFromFile();
        } catch (Exception e) {
            throw new RuntimeException("Error in the loading of the job from the jar: " + e.getMessage(), e);
        }
        // JobClassLoaderOld.loadJob(jarId, mainClass);
        final DataProviderClient<D> provider = jobConfig.getDataProvider();
        final SerializableMapper<D, V> mapper = jobConfig.getMapper();

        // 2nd step: fetch data
        final List<D> data;

        try (final DataProviderClient<D> autoProvider = provider) {
            autoProvider.init();
            try {
                data = autoProvider.fetchChunk(request.getOffset(), request.getLimit());
            } catch (IOException e) {
                throw new RuntimeException("Error fetching data from DataProvider: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error initializing DataProviderClient: " + e.getMessage(), e);
        }

        // 3rd step: map data
        final Map<String, List<V>> mappedData = data.parallelStream().map(mapper::apply).flatMap(Collection::stream)
                // Sorting and partitioning
                .collect(Collectors.groupingBy(Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList())));

        // 4th step: return mapped data
        return mappedData;
    }

}

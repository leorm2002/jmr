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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.exceptions.JMRException;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerExecutor.class);

    public static <D extends Serializable, V extends Serializable, O extends Serializable> void executeReduce(SubmitReduceTaskRequest request)
            throws JMRException {
        LOGGER.info("Executing reduce task for job {}", request.getJobId());
        try (JobClassLoaderOld classLoader = new JobClassLoaderOld(request.getJarId())) {
            String jarId = request.getJarId();
            JobConfiguration<D, V, O> jobConfig;
            try {
                jobConfig = new JobClassLoader(jarId, "job.ser").deserializeFromFile();
            } catch (Exception e) {
                LOGGER.error("Error loading job from jar", e);
                throw new JMRException("Error loading job from jar: " + e.getMessage(), e);
            }

            final SerializableReducer<V, O> reducer = jobConfig.getReducer();
            // Retrieve data from intermediate locations
            List<Pair<String, List<V>>> mappedData = Collections.emptyList();

            // Execute the reduction

            Set<Entry<String, List<List<V>>>> partitionedData = mappedData.parallelStream()
                    .collect(Collectors.groupingByConcurrent(Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList()))).entrySet();

            List<Pair<String, O>> reducedData = partitionedData.parallelStream()
                    .map(e -> reducer.apply(e.getKey(), e.getValue().stream().flatMap(List::stream).collect(Collectors.toList())))
                    .collect(Collectors.toList());

            LOGGER.info("Reduce task for job {} completed", request.getJobId());

        } catch (Exception e) {
            LOGGER.error("Error executing reduce task for job {}", request.getJobId(), e);
            throw new JMRException("Error executing reduce task", e);
        }
    }

    /**
     * Executes the map task as per the request
     */
    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> Map<String, List<V>> executeMap(
            SubmitMapTaskRequest request, WorkerContext ctx) throws JMRException {
        LOGGER.info("Executing map task {} for job {}", request.getTaskId(), request.getJobId());
        // 1. Load the job from the jar
        // 2. Retrieve data from the DataProvider
        // 3. Execute the mapping
        // 4. Save the intermediate data

        // 1st step: load the job configuration
        final String jarId = request.getJarId();

        final String jarPath = ctx.jarStorage.get(jarId);
        final String jobPath = ctx.jobStorage.get(request.getJobId());

        JobConfiguration<D, V, O> jobConfig;
        try {
            jobConfig = new JobClassLoader(jarPath, jobPath).deserializeFromFile();
        } catch (Exception e) {
            LOGGER.error("Error loading job from jar", e);
            throw new JMRException("Error loading job from jar: " + e.getMessage(), e);
        }

        final DataProviderClient<D> provider = jobConfig.getDataProvider();
        final SerializableMapper<D, V> mapper = jobConfig.getMapper();

        // 2nd step: fetch data
        final List<D> data;

        try (final DataProviderClient<D> autoProvider = provider) {
            autoProvider.init();
            try {
                LOGGER.debug("Fetching chunk from offset {} with limit {}", request.getOffset(), request.getLimit());
                data = autoProvider.fetchChunk(request.getOffset(), request.getLimit());
                LOGGER.debug("Fetched {} records", data.size());
            } catch (JMRException e) {
                LOGGER.error("Error fetching data from DataProvider", e);
                throw new JMRException("Error fetching data from DataProvider: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            LOGGER.error("Error initializing DataProviderClient", e);
            throw new JMRException("Error initializing DataProviderClient: " + e.getMessage(), e);
        }

        // 3rd step: map data
        LOGGER.debug("Mapping data...");
        final Map<String, List<V>> mappedData = data.parallelStream().map(mapper::apply).flatMap(Collection::stream)
                // Sorting and partitioning
                .collect(Collectors.groupingBy(Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList())));
        LOGGER.info("Map task {} for job {} completed", request.getTaskId(), request.getJobId());
        // 4th step: return mapped data
        return mappedData;
    }

}

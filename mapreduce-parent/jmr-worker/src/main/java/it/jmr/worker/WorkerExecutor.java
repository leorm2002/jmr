package it.jmr.worker;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.jmr.common.IntermediateDataFetcher;
import it.jmr.common.exceptions.JMRException;
import it.jmr.common.jarservice.JobClassLoader;
import it.jmr.common.models.JobConfiguration;
import it.jmr.common.models.SerializableMapper;
import it.jmr.common.models.SerializableReducer;
import it.jmr.common.providers.DataProviderClient;
import it.jmr.common.utils.Pair;
import it.jmr.grpc.worker.IntermediateDataLocation;
import it.jmr.grpc.worker.SubmitMapTaskRequest;
import it.jmr.grpc.worker.SubmitReduceTaskRequest;
import it.jmr.worker.models.WorkerContext;

/**
 * Static class that executes the map and reduce tasks
 */
public class WorkerExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerExecutor.class);

    public static <D extends Serializable, V extends Serializable, O extends Serializable> List<Pair<String, O>> executeReduce(
            SubmitReduceTaskRequest request, WorkerContext ctx, IntermediateDataFetcher dataFetcher) throws JMRException {
        LOGGER.info("Executing reduce task for job {}", request.getJobId());

        final String jarId = request.getJarId();
        final Path jarPath = ctx.jarStorage.get(jarId);
        final Path jobPath = ctx.jobStorage.get(request.getJobId());

        // 1. Deserialize job configuration
        final JobConfiguration<D, V, O> jobConfig = deserializeJobConfig(jarPath, jobPath);
        final SerializableReducer<V, O> reducer = jobConfig.getReducer();

        // 2. Fetch intermediate mapped data
        final List<Pair<String, List<V>>> mappedData = fetchIntermediateData(request, dataFetcher);

        // 3. Execute the reduction
        final Set<Entry<String, List<List<V>>>> partitionedData = mappedData.parallelStream()
                .collect(Collectors.groupingByConcurrent(Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList()))).entrySet();

        final List<Pair<String, O>> reducedData = partitionedData.parallelStream()
                .map(e -> reducer.apply(e.getKey(), e.getValue().stream().flatMap(List::stream).collect(Collectors.toList())))
                .collect(Collectors.toList());

        // 4. Return reduced data
        LOGGER.info("Reduce task for job {} completed", request.getJobId());
        return reducedData;

    }

    /**
     * Executes the map task as per the request
     */
    public static <D extends Serializable, K extends Serializable, V extends Serializable, O extends Serializable> Map<String, List<V>> executeMap(
            SubmitMapTaskRequest request, WorkerContext ctx) throws JMRException {
        LOGGER.info("Executing map task {} for job {}", request.getTaskId(), request.getJobId());

        final String jarId = request.getJarId();
        final Path jarPath = ctx.jarStorage.get(jarId);
        final Path jobPath = ctx.jobStorage.get(request.getJobId());

        // 1. Deserialize job configuration
        final JobConfiguration<D, V, O> jobConfig = deserializeJobConfig(jarPath, jobPath);
        final DataProviderClient<D> provider = jobConfig.getDataProvider();
        final SerializableMapper<D, V> mapper = jobConfig.getMapper();

        // 2. Fetch data
        final List<D> data = fetchData(request, provider);

        // 3. Map data locally
        LOGGER.debug("Mapping data...");
        final Map<String, List<V>> mappedData = data.parallelStream().map(mapper::apply).flatMap(Collection::stream)
                // Sorting and partitioning
                .collect(Collectors.groupingBy(Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList())));
        LOGGER.info("Map task {} for job {} completed", request.getTaskId(), request.getJobId());

        // 4. Return mapped data
        return mappedData;
    }

    // #region utils
    private static <V extends Serializable> List<Pair<String, List<V>>> fetchIntermediateData(SubmitReduceTaskRequest request,
            IntermediateDataFetcher dataFetcher) throws JMRException {

        final List<Pair<String, List<V>>> mappedData = new java.util.ArrayList<>();

        for (final IntermediateDataLocation location : request.getLocationsList()) {
            final List<V> fetchedData = dataFetcher.fetchIntermediateData(location.getWorkerId(), location.getHost(), location.getPort(),
                    location.getTaskId(), location.getPartitionId());
            mappedData.add(Pair.of(location.getPartitionId(), fetchedData));
        }
        return mappedData;
    }

    private static <D extends Serializable> List<D> fetchData(SubmitMapTaskRequest request, final DataProviderClient<D> provider)
            throws JMRException {
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
        return data;
    }

    private static <D extends Serializable, V extends Serializable, O extends Serializable> JobConfiguration<D, V, O> deserializeJobConfig(
            final Path jarPath, final Path jobPath) throws JMRException {
        final JobConfiguration<D, V, O> jobConfig;
        JobClassLoader loader = new JobClassLoader(jarPath, jobPath);
        try {
            jobConfig = loader.deserializeFromFile();
        } catch (Exception e) {
            LOGGER.error("Error loading job from jar", e);
            throw new JMRException("Error loading job from jar: " + e.getMessage(), e);
        }
        return jobConfig;
    }

}

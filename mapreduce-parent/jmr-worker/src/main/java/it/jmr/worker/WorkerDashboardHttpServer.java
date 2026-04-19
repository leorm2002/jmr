package it.jmr.worker;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import it.jmr.common.WorkerTaskStatus;
import it.jmr.common.utils.Pair;
import it.jmr.worker.models.ReduceTaskResult;
import it.jmr.worker.models.TaskResult;
import it.jmr.worker.models.WorkerContext;

class WorkerDashboardHttpServer implements AutoCloseable {
    private final HttpServer httpServer;
    private final WorkerContext ctx;

    WorkerDashboardHttpServer(final int port, final WorkerContext ctx) throws IOException {
        this.ctx = ctx;
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.httpServer.createContext("/", this::handleIndex);
        this.httpServer.createContext("/api/state", this::handleState);
        this.httpServer.setExecutor(null);
    }

    void start() {
        httpServer.start();
    }

    void stop() {
        httpServer.stop(0);
    }

    @Override
    public void close() {
        stop();
    }

    private void handleIndex(final HttpExchange exchange) throws IOException {
        writeResponse(exchange, 200, "text/html; charset=utf-8", INDEX_HTML);
    }

    private void handleState(final HttpExchange exchange) throws IOException {
        writeResponse(exchange, 200, "application/json; charset=utf-8", buildStateJson());
    }

    private String buildStateJson() {
        final StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"workerId\":\"").append(escape(ctx.workerId)).append("\",");
        json.append("\"grpcPort\":").append(ctx.port).append(",");
        json.append("\"busy\":").append(ctx.isBusy()).append(",");
        final Runtime runtime = Runtime.getRuntime();
        final long heapUsedMb = bytesToMb(runtime.totalMemory() - runtime.freeMemory());
        final long heapCommittedMb = bytesToMb(runtime.totalMemory());
        final long heapMaxMb = bytesToMb(runtime.maxMemory());
        final long heapAvailableMb = Math.max(0L, heapMaxMb - heapUsedMb);
        final int heapUsagePercent = heapMaxMb > 0 ? (int) Math.min(100L, (heapUsedMb * 100L) / heapMaxMb) : 0;
        json.append("\"heapUsedMb\":").append(heapUsedMb).append(",");
        json.append("\"heapCommittedMb\":").append(heapCommittedMb).append(",");
        json.append("\"heapMaxMb\":").append(heapMaxMb).append(",");
        json.append("\"heapAvailableMb\":").append(heapAvailableMb).append(",");
        json.append("\"heapUsagePercent\":").append(heapUsagePercent).append(",");
        json.append("\"mapCompleted\":").append(ctx.mapTaskResults.size()).append(",");
        json.append("\"reduceCompleted\":").append(ctx.reduceTaskResults.size()).append(",");

        final List<Entry<Pair<String, String>, WorkerTaskStatus>> runningTasks = ctx.statusMap.entrySet().stream()
                .filter(entry -> entry.getValue() == WorkerTaskStatus.RUNNING)
                .sorted(Comparator.comparing(entry -> entry.getKey().getSecond())).toList();
        json.append("\"runningTasks\":[");
        for (int index = 0; index < runningTasks.size(); index++) {
            final Entry<Pair<String, String>, WorkerTaskStatus> entry = runningTasks.get(index);
            if (index > 0) {
                json.append(",");
            }
            json.append("{");
            json.append("\"jobId\":\"").append(escape(entry.getKey().getFirst())).append("\",");
            json.append("\"taskId\":\"").append(escape(entry.getKey().getSecond())).append("\"");
            json.append("}");
        }
        json.append("],");

        json.append("\"recentMapTasks\":[");
        final List<TaskResult> recentMapTasks = ctx.mapTaskResults.values().stream().sorted(Comparator.comparingLong(TaskResult::getExecutionTime).reversed())
                .limit(8).toList();
        for (int index = 0; index < recentMapTasks.size(); index++) {
            final TaskResult taskResult = recentMapTasks.get(index);
            if (index > 0) {
                json.append(",");
            }
            json.append("{");
            json.append("\"taskId\":\"").append(escape(taskResult.getTaskId())).append("\",");
            json.append("\"executionTime\":").append(taskResult.getExecutionTime()).append(",");
            json.append("\"partitions\":").append(taskResult.getPartitions().size());
            json.append("}");
        }
        json.append("],");

        json.append("\"recentReduceTasks\":[");
        final List<ReduceTaskResult> recentReduceTasks = ctx.reduceTaskResults.values().stream()
                .sorted(Comparator.comparingLong(ReduceTaskResult::getExecutionTime).reversed()).limit(8).toList();
        for (int index = 0; index < recentReduceTasks.size(); index++) {
            final ReduceTaskResult taskResult = recentReduceTasks.get(index);
            if (index > 0) {
                json.append(",");
            }
            json.append("{");
            json.append("\"taskId\":\"").append(escape(taskResult.getTaskId())).append("\",");
            json.append("\"executionTime\":").append(taskResult.getExecutionTime()).append(",");
            json.append("\"records\":").append(taskResult.getRecordCount());
            json.append("}");
        }
        json.append("],");

        json.append("\"events\":[");
        final List<WorkerContext.DashboardEvent> events = ctx.getDashboardEvents();
        for (int index = 0; index < events.size(); index++) {
            final WorkerContext.DashboardEvent event = events.get(index);
            if (index > 0) {
                json.append(",");
            }
            json.append("{");
            json.append("\"timestamp\":\"").append(escape(Instant.ofEpochMilli(event.timestamp()).toString())).append("\",");
            json.append("\"message\":\"").append(escape(event.message())).append("\"");
            json.append("}");
        }
        json.append("],");

        json.append("\"logs\":[");
        final List<WorkerContext.DashboardEvent> logs = ctx.getLogEvents();
        for (int index = 0; index < logs.size(); index++) {
            final WorkerContext.DashboardEvent logEvent = logs.get(index);
            if (index > 0) {
                json.append(",");
            }
            json.append("{");
            json.append("\"timestamp\":\"").append(escape(Instant.ofEpochMilli(logEvent.timestamp()).toString())).append("\",");
            json.append("\"message\":\"").append(escape(logEvent.message())).append("\"");
            json.append("}");
        }
        json.append("]");
        json.append("}");
        return json.toString();
    }

    private static void writeResponse(final HttpExchange exchange, final int statusCode, final String contentType, final String body)
            throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(bytes);
        }
    }

    private static String escape(final String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\r", "\\r").replace("\n", "\\n");
    }

    private static long bytesToMb(final long bytes) {
        return bytes / (1024L * 1024L);
    }

    private static final String INDEX_HTML = """
            <!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <title>jMR Worker Dashboard</title>
              <style>
                body { margin:0; padding:12px; background:#c0c0c0; color:#000; font:13px "Courier New", monospace; }
                main { max-width:980px; margin:0 auto; }
                h1 { margin:0 0 8px; font-size:24px; }
                h2 { margin:0 0 8px; font-size:16px; }
                .topbar { border:2px outset #fff; background:#d4d0c8; padding:8px; margin-bottom:12px; }
                .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:8px; margin-bottom:12px; }
                .panel { border:2px outset #fff; background:#d4d0c8; padding:8px; }
                .metric { font-size:26px; font-weight:bold; margin-top:4px; }
                .muted { color:#333; }
                .events { max-height:360px; overflow:auto; background:#fff; border:1px solid #000; padding:4px; }
                .event { padding:4px 0; border-bottom:1px dotted #666; }
                table { width:100%; border-collapse:collapse; background:#fff; }
                th { background:#000080; color:#fff; text-align:left; padding:4px; border:1px solid #000; }
                td { text-align:left; padding:4px; border:1px solid #000; vertical-align:top; }
                .row { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
                @media (max-width: 900px) { .row { grid-template-columns:1fr; } }
              </style>
            </head>
            <body>
              <main>
                <div class="topbar">
                  <h1>jMR WORKER CONSOLE</h1>
                  <div class="muted">Local worker state, active tasks and recent execution history.</div>
                </div>
                <section class="grid">
                  <div class="panel"><div class="muted">Worker</div><div class="metric" id="workerId">-</div></div>
                  <div class="panel"><div class="muted">Busy</div><div class="metric" id="busy">-</div></div>
                  <div class="panel"><div class="muted">Map done</div><div class="metric" id="mapCompleted">0</div></div>
                  <div class="panel"><div class="muted">Reduce done</div><div class="metric" id="reduceCompleted">0</div></div>
                  <div class="panel"><div class="muted">Heap used</div><div class="metric" id="heapUsedMb">0 MB</div></div>
                  <div class="panel"><div class="muted">Heap free</div><div class="metric" id="heapAvailableMb">0 MB</div></div>
                  <div class="panel"><div class="muted">Heap max</div><div class="metric" id="heapMaxMb">0 MB</div></div>
                  <div class="panel"><div class="muted">Heap use</div><div class="metric" id="heapUsagePercent">0%</div></div>
                </section>
                <section class="row">
                  <div class="panel">
                    <h2>RUNNING TASKS</h2>
                    <div id="runningTasks" class="muted">No running task.</div>
                  </div>
                  <div class="panel">
                    <h2>RECENT EVENTS</h2>
                    <div class="events" id="events"></div>
                  </div>
                </section>
                <section class="row" style="margin-top:16px;">
                  <div class="panel">
                    <h2>RECENT MAP TASKS</h2>
                    <table><thead><tr><th>Task</th><th>Ms</th><th>Partitions</th></tr></thead><tbody id="recentMapTasks"></tbody></table>
                  </div>
                  <div class="panel">
                    <h2>RECENT REDUCE TASKS</h2>
                    <table><thead><tr><th>Task</th><th>Ms</th><th>Records</th></tr></thead><tbody id="recentReduceTasks"></tbody></table>
                  </div>
                </section>
                <section class="row" style="margin-top:16px;">
                  <div class="panel" style="grid-column: 1 / -1;">
                    <h2>SYSTEM LOGS</h2>
                    <div class="events" id="logs" style="height:250px"></div>
                  </div>
                </section>
              </main>
              <script>
                async function refresh() {
                  const response = await fetch('/api/state', { cache: 'no-store' });
                  const state = await response.json();
                  document.getElementById('workerId').textContent = state.workerId;
                  document.getElementById('busy').textContent = state.busy ? 'YES' : 'NO';
                  document.getElementById('mapCompleted').textContent = state.mapCompleted;
                  document.getElementById('reduceCompleted').textContent = state.reduceCompleted;
                  document.getElementById('heapUsedMb').textContent = `${state.heapUsedMb} MB`;
                  document.getElementById('heapAvailableMb').textContent = `${state.heapAvailableMb} MB`;
                  document.getElementById('heapMaxMb').textContent = `${state.heapMaxMb} MB`;
                  document.getElementById('heapUsagePercent').textContent = `${state.heapUsagePercent}%`;
                  document.getElementById('runningTasks').innerHTML = state.runningTasks.length
                    ? state.runningTasks.map(task => `<div class="event"><strong>${task.taskId}</strong><br><span class="muted">${task.jobId}</span></div>`).join('')
                    : '<span class="muted">No running task.</span>';
                  document.getElementById('events').innerHTML = state.events.slice().reverse().map(event =>
                    `<div class="event"><div class="muted">${event.timestamp}</div><div>${event.message}</div></div>`
                  ).join('');
                  document.getElementById('recentMapTasks').innerHTML = state.recentMapTasks.map(task =>
                    `<tr><td>${task.taskId}</td><td>${task.executionTime}</td><td>${task.partitions}</td></tr>`
                  ).join('');
                  document.getElementById('recentReduceTasks').innerHTML = state.recentReduceTasks.map(task =>
                    `<tr><td>${task.taskId}</td><td>${task.executionTime}</td><td>${task.records}</td></tr>`
                  ).join('');
                  document.getElementById('logs').innerHTML = state.logs.slice().reverse().map(event =>
                    `<div class="event"><div class="muted">${event.timestamp}</div><div><pre style="margin:0">${event.message}</pre></div></div>`
                  ).join('');
                }
                refresh();
                setInterval(refresh, 1500);
              </script>
            </body>
            </html>
            """;
}

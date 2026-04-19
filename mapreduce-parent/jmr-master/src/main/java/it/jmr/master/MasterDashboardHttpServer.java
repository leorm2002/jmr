package it.jmr.master;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import it.jmr.grpc.JobStatus;
import it.jmr.master.models.JobInfoInternal;
import it.jmr.master.models.Worker;

class MasterDashboardHttpServer implements AutoCloseable {
    private final HttpServer httpServer;
    private final MasterContext ctx;

    MasterDashboardHttpServer(final int port, final MasterContext ctx) throws IOException {
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
        final String payload = buildStateJson();
        writeResponse(exchange, 200, "application/json; charset=utf-8", payload);
    }

    private String buildStateJson() {
        final StringBuilder json = new StringBuilder();
        final JobInfoInternal activeJob = ctx.getActiveJob();
        final MasterExecutor.JobExecutionContext<?> activeContext = ctx.getActiveJobContext();

        json.append("{");
        json.append("\"generatedAt\":\"").append(escape(Instant.now().toString())).append("\",");
        json.append("\"phase\":\"").append(escape(ctx.getActivePhase())).append("\",");
        json.append("\"pendingJobs\":").append(ctx.jobQueue.size()).append(",");

        json.append("\"activeJob\":");
        if (activeJob == null || activeContext == null) {
            json.append("null");
        } else {
            json.append("{");
            json.append("\"jobId\":\"").append(escape(activeJob.getJobId())).append("\",");
            json.append("\"status\":\"").append(activeJob.getStatus()).append("\",");
            json.append("\"map\":{\"queued\":").append(activeContext.unassignedMapQueue.size()).append(",\"assigned\":")
                    .append(activeContext.assignedMapTasks.size()).append(",\"completed\":").append(activeContext.completedMapTasks.size())
                    .append(",\"total\":").append(activeContext.originalMapTasks.size()).append("},");
            json.append("\"reduce\":{\"queued\":").append(activeContext.unassignedReduceQueue.size()).append(",\"assigned\":")
                    .append(activeContext.assignedReduceTasks.size()).append(",\"completed\":").append(activeContext.completedReduceTasks.size())
                    .append(",\"total\":").append(activeContext.originalReduceTasks.size()).append("}");
            json.append("}");
        }
        json.append(",");

        json.append("\"workers\":[");
        final List<Worker> workers = ctx.workers.stream().sorted(Comparator.comparing(Worker::getWorkerId)).toList();
        for (int index = 0; index < workers.size(); index++) {
            final Worker worker = workers.get(index);
            if (index > 0) {
                json.append(",");
            }
            json.append("{");
            json.append("\"workerId\":\"").append(escape(worker.getWorkerId())).append("\",");
            json.append("\"grpcPort\":").append(worker.getPort()).append(",");
            json.append("\"dashboardPort\":").append(worker.getPort() + 1000).append(",");
            json.append("\"alive\":true");
            json.append("}");
        }
        json.append("],");

        json.append("\"jobs\":[");
        final List<JobInfoInternal> jobs = ctx.jobs.values().stream().sorted(Comparator.comparingLong(JobInfoInternal::getSubmissionTime).reversed())
                .toList();
        for (int index = 0; index < jobs.size(); index++) {
            final JobInfoInternal job = jobs.get(index);
            if (index > 0) {
                json.append(",");
            }
            json.append("{");
            json.append("\"jobId\":\"").append(escape(job.getJobId())).append("\",");
            json.append("\"status\":\"").append(job.getStatus()).append("\",");
            json.append("\"mapProgress\":").append(job.getMapProgress()).append(",");
            json.append("\"reduceProgress\":").append(job.getReduceProgress()).append(",");
            json.append("\"errorMessage\":\"").append(escape(job.getErrorMessage())).append("\"");
            json.append("}");
        }
        json.append("],");

        json.append("\"events\":[");
        final List<MasterContext.DashboardEvent> events = ctx.getDashboardEvents();
        for (int index = 0; index < events.size(); index++) {
            final MasterContext.DashboardEvent event = events.get(index);
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
        final List<MasterContext.DashboardEvent> logs = ctx.getLogEvents();
        for (int index = 0; index < logs.size(); index++) {
            final MasterContext.DashboardEvent logEvent = logs.get(index);
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

    private static final String INDEX_HTML = """
            <!doctype html>
            <html lang="en">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <title>jMR Dashboard</title>
              <style>
                body { margin:0; padding:12px; background:#c0c0c0; color:#000; font:13px "Courier New", monospace; }
                main { max-width:1280px; margin:0 auto; }
                h1 { margin:0 0 8px; font-size:24px; }
                h2 { margin:0 0 8px; font-size:16px; }
                .topbar { border:2px outset #fff; background:#d4d0c8; padding:8px; margin-bottom:12px; }
                .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:8px; margin-bottom:12px; }
                .panel { border:2px outset #fff; background:#d4d0c8; padding:8px; }
                .metric { font-size:26px; font-weight:bold; margin-top:4px; }
                .muted { color:#333; }
                .row { display:grid; grid-template-columns:2fr 1fr; gap:8px; }
                table { width:100%; border-collapse:collapse; background:#fff; }
                th { background:#000080; color:#fff; text-align:left; padding:4px; border:1px solid #000; }
                td { padding:4px; border:1px solid #000; vertical-align:top; }
                .events { max-height:360px; overflow:auto; background:#fff; border:1px solid #000; padding:4px; }
                .event { padding:4px 0; border-bottom:1px dotted #666; }
                a { color:#0000ee; text-decoration:underline; }
                code { font:12px "Courier New", monospace; }
                @media (max-width: 900px) { .row { grid-template-columns:1fr; } }
              </style>
            </head>
            <body>
              <main>
                <div class="topbar">
                  <h1>jMR MASTER CONSOLE</h1>
                  <div class="muted">Live cluster state, queue sizes, active execution and recent events.</div>
                </div>
                <section class="grid">
                  <div class="panel"><div class="muted">Phase</div><div class="metric" id="phase">-</div></div>
                  <div class="panel"><div class="muted">Pending jobs</div><div class="metric" id="pendingJobs">0</div></div>
                  <div class="panel"><div class="muted">Active job</div><div class="metric" id="activeJob">-</div></div>
                  <div class="panel"><div class="muted">Workers alive</div><div class="metric" id="workersAlive">0</div></div>
                </section>
                <section class="row">
                  <div class="panel">
                    <h2>CURRENT EXECUTION</h2>
                    <div id="currentExecution" class="muted">No active job.</div>
                  </div>
                  <div class="panel">
                    <h2>WORKERS</h2>
                    <div id="workers"></div>
                  </div>
                </section>
                <section class="row" style="margin-top:16px;">
                  <div class="panel">
                    <h2>JOBS</h2>
                    <table>
                      <thead><tr><th>Job</th><th>Status</th><th>Map</th><th>Reduce</th></tr></thead>
                      <tbody id="jobs"></tbody>
                    </table>
                  </div>
                  <div class="panel">
                    <h2>RECENT EVENTS</h2>
                    <div class="events" id="events"></div>
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
                  document.getElementById('phase').textContent = state.phase;
                  document.getElementById('pendingJobs').textContent = state.pendingJobs;
                  document.getElementById('activeJob').textContent = state.activeJob ? state.activeJob.jobId : '-';
                  document.getElementById('workersAlive').textContent = state.workers.filter(w => w.alive).length + '/' + state.workers.length;

                  const workers = document.getElementById('workers');
                  workers.innerHTML = state.workers.map(worker => {
                    const url = `http://${window.location.hostname}:${worker.dashboardPort}`;
                    return `<div class="event"><strong>${worker.workerId}</strong> ${worker.alive ? 'alive' : 'down'}<br><span class="muted">gRPC ${worker.grpcPort} | <a href="${url}" target="_blank">worker dashboard</a></span></div>`;
                  }).join('');

                  const current = document.getElementById('currentExecution');
                  if (!state.activeJob) {
                    current.innerHTML = '<span class="muted">No active job.</span>';
                  } else {
                    current.innerHTML = `
                      <div><strong>${state.activeJob.jobId}</strong> (${state.activeJob.status})</div>
                      <div class="muted">Map queue ${state.activeJob.map.queued} | assigned ${state.activeJob.map.assigned} | completed ${state.activeJob.map.completed}/${state.activeJob.map.total}</div>
                      <div class="muted">Reduce queue ${state.activeJob.reduce.queued} | assigned ${state.activeJob.reduce.assigned} | completed ${state.activeJob.reduce.completed}/${state.activeJob.reduce.total}</div>
                    `;
                  }

                  document.getElementById('jobs').innerHTML = state.jobs.map(job =>
                    `<tr><td><code>${job.jobId}</code></td><td>${job.status}</td><td>${job.mapProgress}%</td><td>${job.reduceProgress}%</td></tr>`
                  ).join('');

                  document.getElementById('events').innerHTML = state.events.slice().reverse().map(event =>
                    `<div class="event"><div class="muted">${event.timestamp}</div><div>${event.message}</div></div>`
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

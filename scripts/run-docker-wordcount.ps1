param(
    [switch]$SkipBuild,
    [switch]$StopCluster
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptRoot "..")
$clusterComposeFile = Join-Path $repoRoot "docker-compose.yml"
$submitterComposeFile = Join-Path $repoRoot "docker-compose.submitter.yml"
$submitterEnvFile = Join-Path $repoRoot "submitter.env"
$submitterProjectName = "jmr-submitter"
$resultDirectory = Join-Path $repoRoot "docker-output"
$resultFileName = "wordcount-result.ser"
$resultFile = Join-Path $resultDirectory $resultFileName

function Invoke-Step {
    param(
        [string]$Message,
        [scriptblock]$Action
    )

    Write-Host "==> $Message"
    & $Action
}

function Test-TcpPort {
    param(
        [string]$TargetHost,
        [int]$Port
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $asyncResult = $client.BeginConnect($TargetHost, $Port, $null, $null)
        if (-not $asyncResult.AsyncWaitHandle.WaitOne(1000, $false)) {
            return $false
        }

        $client.EndConnect($asyncResult)
        return $true
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

function Wait-ForMaster {
    param(
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-TcpPort -TargetHost "localhost" -Port 50050) {
            return
        }

        Start-Sleep -Seconds 2
    }

    throw "Master did not become reachable on localhost:50050 within $TimeoutSeconds seconds."
}

function Open-Dashboards {
    $urls = @(
        "http://localhost:51050"
    )

    foreach ($url in $urls) {
        try {
            Start-Process $url | Out-Null
        } catch {
            Write-Warning "Failed to open dashboard ${url}: $($_.Exception.Message)"
        }
    }
}

function Assert-DockerEngineAvailable {
    cmd /c "docker info >nul 2>nul" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine is not available. Start Docker Desktop or the Docker daemon first."
    }
}

New-Item -ItemType Directory -Force -Path $resultDirectory | Out-Null
if (Test-Path $resultFile) {
    Remove-Item -Force $resultFile
}

if (-not $SkipBuild) {
    Invoke-Step "Pulling cluster images from Docker Hub" {
        docker compose -f $clusterComposeFile pull
    }

    Invoke-Step "Pulling submitter image from Docker Hub" {
        docker compose --env-file $submitterEnvFile -p $submitterProjectName -f $submitterComposeFile pull
    }
}

Invoke-Step "Checking Docker engine" {
    Assert-DockerEngineAvailable
}

Invoke-Step "Stopping any previous cluster" {
    docker compose -f $clusterComposeFile down --remove-orphans
}

Invoke-Step "Starting master and workers" {
    docker compose -f $clusterComposeFile up -d worker-a worker-b worker-c master
}

Invoke-Step "Waiting for master port" {
    Wait-ForMaster -TimeoutSeconds 60
}

Invoke-Step "Opening dashboards" {
    Open-Dashboards
}

$submitterExitCode = 0
$env:SUBMITTER_OUTPUT_DIR = (Resolve-Path $resultDirectory).Path
$env:SUBMITTER_RESULT_FILE = $resultFileName
$env:SUBMITTER_MASTER_HOST = "jmr-master"
$env:SUBMITTER_MASTER_PORT = "50050"
$env:SUBMITTER_CLIENT_ID = "submitter"
try {
    Invoke-Step "Submitting the word count job" {
        docker compose --env-file $submitterEnvFile -p $submitterProjectName -f $submitterComposeFile up --abort-on-container-exit --exit-code-from submitter submitter
        if ($LASTEXITCODE -ne 0) {
            throw "Submitter exited with code $LASTEXITCODE."
        }
    }
} catch {
    $submitterExitCode = 1
    Write-Host "==> Cluster logs after submitter failure"
    docker compose -f $clusterComposeFile logs --tail 200 master worker-a worker-b worker-c
    Write-Host "==> Submitter logs after submitter failure"
    docker compose --env-file $submitterEnvFile -p $submitterProjectName -f $submitterComposeFile logs --tail 200 submitter-input-init submitter
    throw
} finally {
    Invoke-Step "Cleaning submitter resources" {
        docker compose --env-file $submitterEnvFile -p $submitterProjectName -f $submitterComposeFile down -v --remove-orphans
    }
    Remove-Item Env:SUBMITTER_OUTPUT_DIR -ErrorAction SilentlyContinue
    Remove-Item Env:SUBMITTER_RESULT_FILE -ErrorAction SilentlyContinue
    Remove-Item Env:SUBMITTER_MASTER_HOST -ErrorAction SilentlyContinue
    Remove-Item Env:SUBMITTER_MASTER_PORT -ErrorAction SilentlyContinue
    Remove-Item Env:SUBMITTER_CLIENT_ID -ErrorAction SilentlyContinue
    if ($StopCluster) {
        Invoke-Step "Stopping cluster" {
            docker compose -f $clusterComposeFile down --remove-orphans
        }
    }
}

if (-not (Test-Path $resultFile)) {
    throw "Word count completed but no result file was produced at $resultFile."
}

Write-Host ""
Write-Host "Word count completed successfully."
Write-Host "Serialized result: $resultFile"
Write-Host "Master endpoint: localhost:50050"
Write-Host "Dashboards: http://localhost:51050 and worker tabs on :51051-:51053"
Write-Host "Cluster still running: $([bool](-not $StopCluster))"

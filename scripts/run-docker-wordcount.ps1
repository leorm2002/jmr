param(
    [switch]$SkipBuild,
    [switch]$StopCluster
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptRoot "..")
$composeFile = Join-Path $repoRoot "docker-compose.yml"
$resultDirectory = Join-Path $repoRoot "docker-output"
$resultFile = Join-Path $resultDirectory "wordcount-result.ser"

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
    Invoke-Step "Building MapReduce modules" {
        Push-Location (Join-Path $repoRoot "mapreduce-parent")
        try {
            mvn -q install -DskipTests
        } finally {
            Pop-Location
        }
    }

    Invoke-Step "Building word count job" {
        Push-Location (Join-Path $repoRoot "my-mapreduce-job")
        try {
            mvn -q package -DskipTests
        } finally {
            Pop-Location
        }
    }
}

Invoke-Step "Checking Docker engine" {
    Assert-DockerEngineAvailable
}

Invoke-Step "Stopping any previous cluster" {
    docker compose -f $composeFile down --remove-orphans
}

Invoke-Step "Building Docker image" {
    docker compose -f $composeFile build
}

Invoke-Step "Starting master and workers" {
    docker compose -f $composeFile up -d worker-a worker-b worker-c master
}

Invoke-Step "Waiting for master port" {
    Wait-ForMaster -TimeoutSeconds 60
}

$submitterExitCode = 0
try {
    Invoke-Step "Submitting the word count job" {
        docker compose -f $composeFile up --no-deps --force-recreate submitter
        if ($LASTEXITCODE -ne 0) {
            throw "Submitter exited with code $LASTEXITCODE."
        }
    }
} catch {
    $submitterExitCode = 1
    Write-Host "==> Cluster logs after submitter failure"
    docker compose -f $composeFile logs --tail 200 master worker-a worker-b worker-c submitter
    throw
} finally {
    if ($StopCluster) {
        Invoke-Step "Stopping cluster" {
            docker compose -f $composeFile down --remove-orphans
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
Write-Host "Cluster still running: $([bool](-not $StopCluster))"

param(
    [string]$Image = "rlespinasse/drawio-desktop-headless"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$generator = Join-Path $scriptRoot "generate_diagrams.py"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python is required to generate the draw.io sources."
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker is required to export the draw.io diagrams."
}

Write-Host "==> Generating .drawio sources"
python $generator

$diagramDir = (Resolve-Path $scriptRoot).Path
$drawioFiles = Get-ChildItem -Path $scriptRoot -Filter *.drawio | Sort-Object Name

foreach ($file in $drawioFiles) {
    $outputName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name) + ".png"
    Write-Host "==> Exporting $($file.Name) -> $outputName"
    docker run --rm `
        -v "${diagramDir}:/data" `
        $Image `
        -x `
        -f png `
        --width 1800 `
        -o "/data/$outputName" `
        "/data/$($file.Name)" | Out-Null
}

Write-Host "==> Diagram export completed"

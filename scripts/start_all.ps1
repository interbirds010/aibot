$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonPath = Join-Path $projectRoot ".venv\Scripts\python.exe"
$logDirectory = Join-Path $projectRoot "logs"

if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw "Virtual environment Python was not found: $pythonPath"
}
New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null

$services = @(
    @{
        Name = "wallet-feeder"
        Match = "-m src.wallet_feeder"
        Arguments = @("-m", "src.wallet_feeder")
        Stdout = "wallet_feeder.stdout.log"
        Stderr = "wallet_feeder.stderr.log"
    },
    @{
        Name = "smart-money-monitor"
        Match = "-m src.monitor"
        Arguments = @("-m", "src.monitor")
        Stdout = "service.stdout.log"
        Stderr = "service.stderr.log"
    },
    @{
        Name = "dashboard"
        Match = "-m streamlit run src/dashboard.py"
        Arguments = @(
            "-m", "streamlit", "run", "src/dashboard.py",
            "--server.address", "localhost", "--server.port", "8501",
            "--server.headless", "true"
        )
        Stdout = "dashboard.stdout.log"
        Stderr = "dashboard.stderr.log"
    }
)

function Find-ServiceProcess([string]$match) {
    @(Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -and
        $_.CommandLine.Contains($projectRoot) -and
        $_.CommandLine.Contains($match)
    })
}

function Archive-Log([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) {
        return
    }
    $item = Get-Item -LiteralPath $path
    if ($item.Length -eq 0) {
        Remove-Item -LiteralPath $path
        return
    }
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $archive = Join-Path $item.DirectoryName "$($item.BaseName).$timestamp$($item.Extension)"
    Move-Item -LiteralPath $path -Destination $archive
}

foreach ($service in $services) {
    $running = @(Find-ServiceProcess $service.Match)
    if ($running.Count -gt 0) {
        Write-Host "$($service.Name) already running (PID $($running.ProcessId -join ', '))"
        continue
    }

    $stdoutPath = Join-Path $logDirectory $service.Stdout
    $stderrPath = Join-Path $logDirectory $service.Stderr
    Archive-Log $stdoutPath
    Archive-Log $stderrPath

    $process = Start-Process `
        -FilePath $pythonPath `
        -ArgumentList $service.Arguments `
        -WorkingDirectory $projectRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -WindowStyle Hidden `
        -PassThru
    Write-Host "$($service.Name) started (PID $($process.Id))"
}

Start-Sleep -Seconds 3
Write-Host "Dashboard: http://localhost:8501/"

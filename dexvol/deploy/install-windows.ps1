<#
.SYNOPSIS
  Install the DEX volume monitor as a Windows scheduled task that starts at
  logon and restarts itself if it dies.

.DESCRIPTION
  Windows has no systemd, and running this in a console window means it dies
  with the window. A scheduled task is the built-in way to get the two
  properties that matter — start automatically, come back after a crash —
  without installing a service wrapper.

  No administrator rights are needed: the task runs as you.

.EXAMPLE
  cd <repo>\dexvol
  powershell -ExecutionPolicy Bypass -File .\deploy\install-windows.ps1
#>

[CmdletBinding()]
param(
    [string]$InstallDir = "$env:LOCALAPPDATA\dexvol",
    [string]$TaskName = "DexVolMonitor"
)

$ErrorActionPreference = 'Stop'

$Repo = Split-Path -Parent $PSScriptRoot

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "Go is not installed or not on PATH. Install it from https://go.dev/dl/"
}

Write-Host "Building..." -ForegroundColor Cyan
Push-Location $Repo
try {
    foreach ($cmd in 'monitor', 'doctor', 'coverage') {
        & go build -o "$Repo\$cmd.exe" "./cmd/$cmd"
        if ($LASTEXITCODE -ne 0) { throw "go build ./cmd/$cmd failed" }
    }
} finally {
    Pop-Location
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
foreach ($cmd in 'monitor', 'doctor', 'coverage') {
    Copy-Item "$Repo\$cmd.exe" -Destination $InstallDir -Force
}

$EnvFile = Join-Path $InstallDir 'dexvol.env'
if (-not (Test-Path $EnvFile)) {
    Copy-Item "$PSScriptRoot\dexvol.env.example" -Destination $EnvFile
    Write-Host ""
    Write-Host "Created $EnvFile" -ForegroundColor Yellow
    Write-Host "Fill in TELEGRAM_BOT_TOKEN and TELEGRAM_OWNER_ID, then run this script again."
    exit 0
}

# Refuse to install something that cannot work. A preflight table beats a task
# that flaps invisibly in the background.
Write-Host ""
Write-Host "Running preflight..." -ForegroundColor Cyan
Push-Location $InstallDir
try {
    & "$InstallDir\doctor.exe"
    $preflight = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($preflight -ne 0) {
    Write-Host ""
    throw "Preflight failed. Fix the failures above and run this script again."
}

$Action = New-ScheduledTaskAction -Execute "$InstallDir\monitor.exe" -WorkingDirectory $InstallDir
$Trigger = New-ScheduledTaskTrigger -AtLogOn

# RestartCount/RestartInterval are the crash recovery; ExecutionTimeLimit must
# be cleared or Windows kills a long-running task after three days, which for a
# monitor means going quiet without a word.
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -DontStopOnIdleEnd `
    -StartWhenAvailable `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -MultipleInstances IgnoreNew

$Principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Settings $Settings -Principal $Principal -Description "DEX Volume Anomaly Monitor" -Force | Out-Null

Start-ScheduledTask -TaskName $TaskName

Write-Host ""
Write-Host "Installed and started." -ForegroundColor Green
Write-Host "  installed in: $InstallDir"
Write-Host "  status:       Get-ScheduledTask -TaskName $TaskName"
Write-Host "  stop:         Stop-ScheduledTask -TaskName $TaskName"
Write-Host "  remove:       Unregister-ScheduledTask -TaskName $TaskName -Confirm:`$false"
Write-Host ""
Write-Host "The task starts at logon. Enable automatic logon if you want it to survive an unattended reboot."

# PowerShell script to download and install bruin for Windows

$ErrorActionPreference = "Stop"

$OWNER = "bruin-data"
$REPO = "bruin"
$BINARY = "bruin.exe"
$BINDIR = if ($env:BINDIR) { $env:BINDIR } else { ".\bin" }
$GITHUB_DOWNLOAD = "https://github.com/${OWNER}/${REPO}/releases/download"

function Log-Info($message) {
    Write-Host "[INFO] $message"
}

function Log-Error($message) {
    Write-Host "[ERROR] $message" -ForegroundColor Red
}

function Get-Arch {
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    switch ($arch) {
        "X64" { return "x86_64" }
        "Arm64" { return "arm64" }
        default { throw "Unsupported architecture: $arch" }
    }
}

function Get-LatestRelease {
    $url = "https://api.github.com/repos/$OWNER/$REPO/releases/latest"
    $release = Invoke-RestMethod -Uri $url
    return $release.tag_name
}

# Main execution starts here
$ARCH = Get-Arch

# Get the latest release if no tag is specified
$TAG = if ($args[0]) { $args[0] } else { Get-LatestRelease }
$VERSION = $TAG -replace '^v', ''

Log-Info "Found version ${VERSION} for ${TAG}/Windows/${ARCH}"

$NAME = "${BINARY}_Windows_${ARCH}.zip"

$DOWNLOAD_URL = "${GITHUB_DOWNLOAD}/${TAG}/${NAME}"

# Create BINDIR
if (!(Test-Path $BINDIR)) {
    New-Item -ItemType Directory -Force -Path $BINDIR | Out-Null
}

$TempFile = [System.IO.Path]::GetTempFileName()
try {
    Log-Info "Downloading from ${DOWNLOAD_URL}"
    Invoke-WebRequest -Uri $DOWNLOAD_URL -OutFile $TempFile

    # Extract the zip file
    Expand-Archive -Path $TempFile -DestinationPath $BINDIR -Force

    $BinaryPath = Join-Path $BINDIR $BINARY
    if (Test-Path $BinaryPath) {
        Log-Info "Successfully installed ${BinaryPath}"
    } else {
        throw "Binary not found after extraction"
    }
}
catch {
    Log-Error "Failed to download or install: $_"
    exit 1
}
finally {
    Remove-Item -Path $TempFile -ErrorAction SilentlyContinue
}

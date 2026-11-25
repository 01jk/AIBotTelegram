$ErrorActionPreference = 'Stop'
Push-Location -LiteralPath $PSScriptRoot
try {
    $venvPython = '.\.venv\Scripts\python.exe'
    $installStamp = '.\.venv\aibottelegram-project.sha256'
    if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
        & python -m venv .venv
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    $projectHash = (Get-FileHash -LiteralPath 'pyproject.toml' -Algorithm SHA256).Hash
    $installedHash = if (Test-Path -LiteralPath $installStamp -PathType Leaf) {
        Get-Content -LiteralPath $installStamp -Raw
    }
    if ($projectHash -ne $installedHash) {
        & $venvPython -m pip install --disable-pip-version-check -e .
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        Set-Content -LiteralPath $installStamp -Value $projectHash -Encoding ascii -NoNewline
    }

    & $venvPython -m aibottelegram run
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

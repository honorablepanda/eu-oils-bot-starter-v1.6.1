# tasks/refresh.ps1  (PowerShell 5.1 safe)
param(
  [string]$Program,
  [int]   $S2Limit,
  [int]   $S5DaysWait,
  [string]$S2Source,
  [int]   $S4Limit,
  [int]   $S5MaxRows,
  [string]$Variant
)

$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

# Defaults: CLI param > env var > hard default
if (-not $Program)     { $Program    = if ($env:PROGRAM)      { $env:PROGRAM }      else { "YourProgramName" } }
if (-not $S2Limit)     { $S2Limit    = if ($env:S2_LIMIT)     { [int]$env:S2_LIMIT } else { 100 } }
if (-not $S5DaysWait)  { $S5DaysWait = if ($env:S5_DAYS_WAIT) { [int]$env:S5_DAYS_WAIT } else { 5 } }
if (-not $S2Source)    { $S2Source   = if ($env:S2_SOURCE)    { $env:S2_SOURCE }    else { "csv" } }
if (-not $S4Limit)     { $S4Limit    = if ($env:S4_LIMIT)     { [int]$env:S4_LIMIT } else { 500 } }
if (-not $S5MaxRows)   { $S5MaxRows  = if ($env:S5_MAX_ROWS)  { [int]$env:S5_MAX_ROWS } else { 500 } }
if (-not $Variant)     { $Variant    = if ($env:VARIANT)      { $env:VARIANT }      else { "default" } }

# Paths
$here   = Split-Path -Parent $MyInvocation.MyCommand.Path
$root   = Split-Path -Parent $here
$py     = Join-Path $root ".venv\Scripts\python.exe"
$script = Join-Path $root "scripts\auto_refresh.py"

# Env
if (-not $env:EOS_NO_OUTLOOK) { $env:EOS_NO_OUTLOOK = "1" }
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

# Run
& "$py" -X utf8 "$script" `
  --with-s2 --with-s3 --with-s4 --with-s5 `
  --s2-limit $S2Limit `
  --s5-days-wait $S5DaysWait `
  --program $Program `
  --s2-source $S2Source `
  --s4-limit $S4Limit `
  --s5-max-rows $S5MaxRows `
  --variant $Variant

exit $LASTEXITCODE

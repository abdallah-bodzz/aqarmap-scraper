<#
.SYNOPSIS
    Runs the first-time pipeline for aqarmap_scraper.py, in order, from the repo root.

.DESCRIPTION
    probe -> crawl -> reparse -> details -> validate -> export.
    The database and exports go to data/. Saved HTML goes to samples/.
    Nothing here deletes files. If the database already exists the steps run against it.

.PARAMETER DryRun
    Print the commands without running them.

.EXAMPLE
    .\scripts\run_full_pipeline.ps1 -DryRun

.EXAMPLE
    .\scripts\run_full_pipeline.ps1 -Rps 0.5 -MaxPages 3
#>
[CmdletBinding()]
param(
    [switch]$DryRun,
    [string]$Python = "python",
    [string]$Db = "data/aqarmap.db",
    [string]$DetailUrl = "https://aqarmap.com.eg/en/listing/7416775-for-rent-alexandria-el-mandara-alex-el-mandara-bahri-sidi-kamal-st/",
    [string]$ResultsUrl = "https://aqarmap.com.eg/en/for-rent/property-type/alexandria/",
    [double]$Rps = 0.5,
    [int]$MaxPages = 3,
    [int]$DetailLimit = 5
)

$ErrorActionPreference = "Stop"

# All paths are relative to the repo root, whatever folder the script is started from.
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
New-Item -ItemType Directory -Force -Path "data", "samples" | Out-Null

function Invoke-Step {
    param(
        [string]$Name,
        [string[]]$StepArgs,
        [int[]]$AllowedExit = @(0)
    )
    $line = "$Python aqarmap_scraper.py " + ($StepArgs -join " ")
    Write-Host ""
    Write-Host "== $Name"
    Write-Host "   $line"
    if ($DryRun) { return }
    & $Python aqarmap_scraper.py @StepArgs
    $code = $LASTEXITCODE
    if ($AllowedExit -notcontains $code) {
        throw "Step '$Name' exited with code $code. See docs/RUNBOOK.md for what that code means."
    }
    Write-Host "   exit code $code"
}

# 1. Proves: the detail page still has a decodable listing object and both JSON-LD blocks,
#    and the parsed record passes the spec checks. Writes nothing to the database. Exit 5 means the page changed.
Invoke-Step -Name "probe detail page" -StepArgs @("probe", "--db", $Db, "--url", $DetailUrl, "--rps", "$Rps")

# 2. Proves: the results page canary passes and rows land in the database.
#    --max-pages stops the cycle early on purpose (exit 0, "cycle complete: NO"). Exit 7 is also accepted so a
#    network hiccup mid-crawl does not stop the later offline steps. Progress is saved either way.
Invoke-Step -Name "crawl results pages" -StepArgs @(
    "crawl", "--db", $Db, "--url", $ResultsUrl, "--rps", "$Rps", "--max-pages", "$MaxPages", "--passes", "1"
) -AllowedExit @(0, 7)

# 3. Proves: every parsed column can be rebuilt from stored data with no network.
Invoke-Step -Name "reparse offline" -StepArgs @("reparse", "--db", $Db)

# 4. Proves: detail pages enrich existing rows without creating duplicates.
#    --url is passed so the script uses the same host as the crawl.
Invoke-Step -Name "details (limited)" -StepArgs @(
    "details", "--db", $Db, "--url", $ResultsUrl, "--limit", "$DetailLimit", "--rps", "$Rps"
)

# 5. Proves: the quality flags and coverage table run on the new data.
Invoke-Step -Name "validate" -StepArgs @("validate", "--db", $Db, "--out", "data/flagged.csv")

# 6. Proves: all export formats write, and the data dictionary lists the new columns.
Invoke-Step -Name "export to data/" -StepArgs @("export", "--db", $Db, "--format", "all", "--out", "data/listings")

Write-Host ""
if ($DryRun) { Write-Host "Dry run. Nothing was executed." } else { Write-Host "Pipeline finished. Output is in data/." }

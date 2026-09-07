<#
.SYNOPSIS
    Windows/PowerShell wrapper around publish_to_tableau_cloud.py.

.DESCRIPTION
    publish_to_tableau_cloud.py is a plain Python script -- this wrapper just
    finds the right Python interpreter, checks that .env exists (the Python
    script reads its Tableau Cloud config from environment variables), and
    then calls the script with whatever flags you passed here. It exists so
    a Windows user can run one PowerShell command instead of remembering to
    activate a venv and type out `python3 publish_to_tableau_cloud.py ...`
    by hand every time.

    What actually talks to Tableau Cloud is still publish_to_tableau_cloud.py
    -- this script contributes no logic of its own beyond "find Python, check
    .env, run the script, forward its exit code."

    Required environment variables (set in a `.env` file in the project root --
    the parent of this scripts/ folder -- see .env.example, which you copy to
    `.env` and fill in):

      TABLEAU_SERVER_URL         Your Tableau Cloud pod URL, e.g.
                                  https://10ax.online.tableau.com
      TABLEAU_SITE_CONTENT_URL   The site's "content URL" slug (the part
                                  after /site/ in the browser address bar --
                                  NOT the site's display name).
      TABLEAU_PROJECT_NAME       Project (folder) to publish into. Must
                                  already exist on the site.
      TABLEAU_TOKEN_NAME         Name of a Tableau Personal Access Token
                                  (create one under Account Settings ->
                                  Personal Access Tokens).
      TABLEAU_TOKEN_SECRET       Secret for that same token.

    The published data source's name is NOT read from `.env` -- pass
    -Target explicitly, or omit it and the Python script derives one from
    the -Source filename instead (underscores become spaces), e.g.
    Finished_Merged.hyper -> "Finished Merged".

    NEVER commit `.env`, paste its contents into chat, or share it -- it
    holds a credential that can publish/overwrite content on your site.

    One-time setup this script does NOT do for you (run these yourself
    first, from the project root -- the parent of this scripts/ folder):

        python -m venv .venv
        .venv\Scripts\Activate.ps1
        pip install -r requirements.txt
        Copy-Item .env.example .env
        notepad .env   # fill in the real values

.PARAMETER Source
    Forwarded as --source: path to the .hyper file to publish. Required --
    there is no default. Omitting it makes this script (and the Python
    script underneath it) fail with an error showing the exact flag format.

.PARAMETER Target
    Forwarded as --target: name to give the published data source on
    Tableau Cloud. Optional -- if omitted, defaults to the -Source filename
    with its extension stripped and underscores replaced with spaces, e.g.
    Finished_Merged.hyper -> "Finished Merged".

.PARAMETER Metadata
    Forwarded as --metadata: path to a metadata JSON file (as produced by
    generate_metadata.py) to apply -- name, description, tags,
    certification, and column descriptions. Optional -- omit it to publish
    with none of that applied. Pointing this at a file that doesn't exist
    is an error, since it means metadata was explicitly requested but
    nothing can be read.

.PARAMETER DryRun
    Forwarded as --dry-run: print what would be published without making
    any network call to Tableau Cloud.

    All examples are run from the project root (the parent of this scripts/
    folder), so the wrapper is invoked as .\scripts\publish_to_tableau_cloud.ps1
    and paths point into the data\ folder.

.EXAMPLE
    .\scripts\publish_to_tableau_cloud.ps1 -Source data\Finished_Merged.hyper
    Publishes data\Finished_Merged.hyper as data source "Finished Merged"
    (derived from the filename) with no metadata applied.

.EXAMPLE
    .\scripts\publish_to_tableau_cloud.ps1 -Source data\Finished_Merged.hyper -Target "Rugby Chains"
    Publishes the same file, but names the data source "Rugby Chains"
    instead of the derived default.

.EXAMPLE
    .\scripts\publish_to_tableau_cloud.ps1 -Source data\Finished_Merged.hyper -Metadata data\datasource_metadata.json -DryRun
    Shows what would be published -- including metadata from
    data\datasource_metadata.json -- without publishing anything.

.EXAMPLE
    .\scripts\publish_to_tableau_cloud.ps1 -Source data\Some_Other_Extract.hyper -Metadata data\datasource_metadata.json
    Publishes a different .hyper file and applies the given metadata file to it.
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Source,

    [Parameter(Position = 1)]
    [string]$Target,

    [Parameter(Position = 2)]
    [string]$Metadata,

    [switch]$DryRun
)

# Stop on the first unhandled error instead of continuing with a half-broken run.
$ErrorActionPreference = "Stop"

# Run from the project root (the parent of this scripts/ folder), not whatever
# directory the caller happened to be in. .venv, .env and requirements.txt live
# in the project root; the Python script lives here in scripts/ alongside this
# wrapper, and it reads its Tableau Cloud config from .env (found in the CWD).
$ProjectRoot = Split-Path $PSScriptRoot -Parent
Push-Location $ProjectRoot
try {
    # Prefer the project's own virtualenv over whatever "python" happens to
    # be on PATH, so this always runs with the exact tableauhyperapi /
    # tableauserverclient / python-dotenv versions pinned in requirements.txt.
    # Checked in both layouts because a venv created on Windows puts its
    # interpreter under .venv\Scripts\, while one created on macOS/Linux
    # (e.g. if this same repo folder is shared from a Mac) puts it under
    # .venv/bin/ -- either can exist depending on where `python -m venv .venv`
    # was originally run. The venv lives in the project root, not scripts/.
    $venvCandidates = @(
        (Join-Path $ProjectRoot ".venv\Scripts\python.exe"),
        (Join-Path $ProjectRoot ".venv/bin/python3"),
        (Join-Path $ProjectRoot ".venv/bin/python")
    )
    $python = $venvCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1

    if (-not $python) {
        Write-Warning ".venv not found in the project root -- falling back to whatever Python is on PATH. Dependencies (tableauhyperapi, tableauserverclient, python-dotenv) must already be installed there, e.g. via 'pip install -r requirements.txt'."
        $onPath = Get-Command python -ErrorAction SilentlyContinue
        if (-not $onPath) { $onPath = Get-Command python3 -ErrorAction SilentlyContinue }
        if (-not $onPath) {
            throw "No Python interpreter found (checked .venv and PATH). Run 'python -m venv .venv; .venv\Scripts\Activate.ps1; pip install -r requirements.txt' first."
        }
        $python = $onPath.Source
    }

    # publish_to_tableau_cloud.py itself checks for these and refuses to make
    # any network call if they're missing -- this check just fails a little
    # faster, with a pointer to .env.example, instead of letting the Python
    # process start up first.
    if (-not (Test-Path (Join-Path $ProjectRoot ".env"))) {
        throw "No .env file found in the project root. Copy .env.example to .env and fill in your Tableau Cloud details first (see this script's help: Get-Help .\scripts\publish_to_tableau_cloud.ps1 -Full)."
    }

    # publish_to_tableau_cloud.py requires --source (no default, and it
    # errors out with the flag format if missing) -- fail here with the
    # same message rather than relying on the Python process to catch it,
    # so a missing -Source is obvious without spawning python at all.
    if (-not $Source) {
        throw "Missing required parameter: -Source <path-to-file>.hyper (forwarded as --source to publish_to_tableau_cloud.py). Example: .\scripts\publish_to_tableau_cloud.ps1 -Source data\Finished_Merged.hyper"
    }

    # Build the argument list exactly as publish_to_tableau_cloud.py's own
    # argparse setup expects:
    #   publish_to_tableau_cloud.py --source=<file> [--target=<name>] [--metadata=<file>] [--dry-run]
    # The script itself lives in scripts/ (next to this wrapper), so it's
    # referenced by absolute path even though the CWD is now the project root.
    $pyArgs = @((Join-Path $PSScriptRoot "publish_to_tableau_cloud.py"), "--source=$Source")
    if ($Target) { $pyArgs += "--target=$Target" }
    if ($Metadata) { $pyArgs += "--metadata=$Metadata" }
    if ($DryRun) { $pyArgs += "--dry-run" }

    Write-Host "Running: $python $($pyArgs -join ' ')" -ForegroundColor Cyan
    & $python @pyArgs

    # Surface the Python process's real exit code to whatever called this
    # script (e.g. a CI step), instead of PowerShell's own (always-0) status.
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

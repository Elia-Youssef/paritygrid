[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$docsRoot = Join-Path $projectRoot 'docs'
$failures = [System.Collections.Generic.List[string]]::new()

function Get-ProjectRelativePath {
    param(
        [Parameter(Mandatory)]
        [string]$LiteralPath
    )

    Push-Location -LiteralPath $projectRoot
    try {
        $relativePath = Resolve-Path -LiteralPath $LiteralPath -Relative
        return $relativePath -replace '^\.[\\/]', ''
    }
    finally {
        Pop-Location
    }
}

$requiredFiles = @(
    'README.md',
    'docs/INDEX.md',
    'docs/PRODUCT.md',
    'docs/ARCHITECTURE.md',
    'docs/DATA_ARCHITECTURE.md',
    'docs/EXECUTION_MODEL.md',
    'docs/API_CONTRACT.md',
    'docs/UI_SPECIFICATION.md',
    'docs/SECURITY.md',
    'docs/ENGINEERING_STANDARDS.md',
    'docs/REPOSITORY_POLICY.md',
    'docs/GIT_WORKFLOW.md',
    'docs/QUALITY_STRATEGY.md',
    'docs/DELIVERY_PLAN.md',
    'docs/WORK_PACKAGES.md',
    'docs/CI_RELEASE.md',
    'docs/PUBLIC_DOCUMENTATION.md',
    'docs/INSTRUCTION_AUDIT.md'
)

foreach ($relativePath in $requiredFiles) {
    $absolutePath = Join-Path $projectRoot $relativePath
    if (-not (Test-Path -LiteralPath $absolutePath -PathType Leaf)) {
        $failures.Add("Missing required file: $relativePath")
    }
}

$repositoryFiles = @(
    git -C $projectRoot ls-files
    git -C $projectRoot ls-files --others --exclude-standard
) | Sort-Object -Unique
$markdownFiles = $repositoryFiles |
    Where-Object { [IO.Path]::GetExtension($_) -ieq '.md' } |
    ForEach-Object { Get-Item -LiteralPath (Join-Path $projectRoot $_) }

$prohibitedPatterns = @(
    ('\b' + 'ag' + 'ents?\b'),
    ('\b' + 'A' + 'I\b'),
    ('artificial' + '\s+' + 'intelligence'),
    ('large' + '\s+' + 'language' + '\s+' + 'model'),
    ('\b' + 'G' + 'PT\b')
)

foreach ($markdownFile in $markdownFiles) {
    $content = Get-Content -LiteralPath $markdownFile.FullName -Raw
    foreach ($pattern in $prohibitedPatterns) {
        if ($content -cmatch $pattern -or $content -match $pattern) {
            $relativePath = Get-ProjectRelativePath -LiteralPath $markdownFile.FullName
            $failures.Add("Prohibited internal terminology in Markdown: $relativePath")
            break
        }
    }

    $links = [regex]::Matches($content, '(?<!\!)\[[^\]]+\]\((?<target>[^)]+)\)')
    foreach ($link in $links) {
        $target = $link.Groups['target'].Value.Trim().Trim('<', '>')
        if ($target -match '^(https?://|mailto:|#)') {
            continue
        }

        $pathPart = ($target -split '#', 2)[0]
        if ([string]::IsNullOrWhiteSpace($pathPart)) {
            continue
        }

        $resolvedPath = Join-Path $markdownFile.DirectoryName $pathPart
        if (-not (Test-Path -LiteralPath $resolvedPath)) {
            $relativePath = Get-ProjectRelativePath -LiteralPath $markdownFile.FullName
            $failures.Add("Broken local link in ${relativePath}: $target")
        }
    }
}

$prohibitedNames = $repositoryFiles |
    Where-Object { [IO.Path]::GetFileName($_) -ieq ('AG' + 'ENTS.md') }
foreach ($prohibitedName in $prohibitedNames) {
    $failures.Add("Prohibited instruction filename: $prohibitedName")
}

$unsafeTrackedExtensions = @('.db', '.duckdb', '.parquet', '.pem', '.key')
$trackedFiles = git -C $projectRoot ls-files
foreach ($trackedFile in $trackedFiles) {
    if ($unsafeTrackedExtensions -contains [IO.Path]::GetExtension($trackedFile).ToLowerInvariant()) {
        $failures.Add("Unsafe tracked runtime or secret file: $trackedFile")
    }
}

$approvedDescription = 'Local-first data reconciliation and I/O execution showcase built with Python, FastAPI, SQLite, DuckDB, React and TypeScript. Inspired by a real-world system that remains confidential; this is an independent implementation using synthetic data.'
$productText = Get-Content -LiteralPath (Join-Path $docsRoot 'PRODUCT.md') -Raw
if (-not $productText.Contains($approvedDescription)) {
    $failures.Add('The approved repository description is missing or changed in docs/PRODUCT.md')
}

if ($failures.Count -gt 0) {
    $failures | ForEach-Object { Write-Error $_ }
    exit 1
}

Write-Output "Instruction validation passed for $($markdownFiles.Count) Markdown files."

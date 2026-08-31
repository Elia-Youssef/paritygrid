[CmdletBinding()]
param(
    [switch]$CandidateFileSelfTest
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$docsRoot = Join-Path $projectRoot 'docs'
$failures = [System.Collections.Generic.List[string]]::new()

function Get-RepositoryCandidateFiles {
    param(
        [Parameter(Mandatory)]
        [string]$Root
    )

    return @(
        git -C $Root ls-files
        git -C $Root ls-files --others --exclude-standard
    ) |
        Sort-Object -Unique |
        Where-Object { Test-Path -LiteralPath (Join-Path $Root $_) -PathType Leaf }
}

if ($CandidateFileSelfTest) {
    $selfTestRoot = Join-Path (
        [IO.Path]::GetTempPath()
    ) ("paritygrid-validator-{0}" -f [Guid]::NewGuid().ToString('N'))
    try {
        $assetRoot = Join-Path $selfTestRoot 'web/dist/assets'
        [void](New-Item -ItemType Directory -Path $assetRoot -Force)
        git -C $selfTestRoot init --quiet
        if ($LASTEXITCODE -ne 0) {
            throw 'Unable to initialize candidate-file self-test repository.'
        }

        $deletedAsset = Join-Path $assetRoot 'index-old.js'
        $replacementAsset = Join-Path $assetRoot 'index-new.js'
        [IO.File]::WriteAllText($deletedAsset, 'old')
        git -C $selfTestRoot add -- 'web/dist/assets/index-old.js'
        if ($LASTEXITCODE -ne 0) {
            throw 'Unable to stage the self-test fixture.'
        }

        Remove-Item -LiteralPath $deletedAsset
        [IO.File]::WriteAllText($replacementAsset, 'new')
        $candidateFiles = @(Get-RepositoryCandidateFiles -Root $selfTestRoot)

        if ($candidateFiles -contains 'web/dist/assets/index-old.js') {
            throw 'Deleted indexed files must not be repository candidates.'
        }
        if ($candidateFiles -notcontains 'web/dist/assets/index-new.js') {
            throw 'Existing untracked files must be repository candidates.'
        }

        Write-Output 'Repository candidate-file self-test passed.'
        return
    }
    finally {
        if (Test-Path -LiteralPath $selfTestRoot) {
            Remove-Item -LiteralPath $selfTestRoot -Recurse -Force
        }
    }
}

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
    '.env.example',
    'README.md',
    'docs/PHASE_STATUS.md',
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
    'docs/INSTRUCTION_AUDIT.md',
    'docs/decisions/0005-concurrent-execution-and-runner-contract.md',
    'docs/decisions/0006-fingerprint-taxonomy.md'
)

foreach ($relativePath in $requiredFiles) {
    $absolutePath = Join-Path $projectRoot $relativePath
    if (-not (Test-Path -LiteralPath $absolutePath -PathType Leaf)) {
        $failures.Add("Missing required file: $relativePath")
    }
}

$repositoryFiles = @(Get-RepositoryCandidateFiles -Root $projectRoot)
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

$trackedFiles = $repositoryFiles
$generatedFileInventory = @(
    'tests/fixtures/persistence/v0001/manifest.json',
    'tests/fixtures/persistence/v0001/schema.sql',
    'tests/fixtures/persistence/v0001/seed.sql',
    'tests/fixtures/sequential_e2e/expected.json',
    'uv.lock',
    'web/package-lock.json',
    'docs/generated/openapi.json',
    'web/src/api/generated/schema.d.ts',
    'web/dist/index.html',
    'web/dist/assets/index-cEiBJWhO.css',
    'web/dist/assets/index-turva_mm.js'
)
$unsafeTrackedExtensions = @(
    '.cer', '.crt', '.db', '.der', '.duckdb', '.key', '.kdbx', '.log', '.parquet',
    '.p12', '.pem', '.pfx', '.ppk', '.pyc', '.pyd', '.pyo', '.sqlite', '.sqlite3',
    '.temp', '.tmp'
)
$unsafeTrackedNames = @('.coverage', '.ds_store', 'coverage.xml', 'desktop.ini', 'thumbs.db')
$unsafeTrackedSuffixes = @('-shm', '-wal', '.db-shm', '.db-wal')
foreach ($trackedFile in $trackedFiles) {
    $normalizedPath = $trackedFile.Replace('\', '/').ToLowerInvariant()
    $fileName = [IO.Path]::GetFileName($normalizedPath)
    $extension = [IO.Path]::GetExtension($normalizedPath)
    $hasUnsafeSuffix = $false
    foreach ($suffix in $unsafeTrackedSuffixes) {
        if ($normalizedPath.EndsWith($suffix, [StringComparison]::OrdinalIgnoreCase)) {
            $hasUnsafeSuffix = $true
            break
        }
    }
    $isEnvironmentFile = $fileName -eq '.env' -or $fileName.StartsWith('.env.')
    $isApprovedEnvironmentExample = $normalizedPath -eq '.env.example'
    $isRuntimeDirectory = $normalizedPath -match '(^|/)(__pycache__|\.hypothesis|\.mypy_cache|\.paritygrid|\.pyright|\.pytest_cache|\.ruff_cache|\.venv|build|coverage|data|dist|htmlcov|node_modules|playwright-report|test-results)(/|$)'
    if (
        $unsafeTrackedExtensions -contains $extension -or
        $unsafeTrackedNames -contains $fileName -or
        $fileName.StartsWith('.coverage.') -or
        $hasUnsafeSuffix -or
        ($isEnvironmentFile -and -not $isApprovedEnvironmentExample) -or
        ($isRuntimeDirectory -and $generatedFileInventory -notcontains $trackedFile)
    ) {
        $failures.Add("Unsafe tracked runtime or secret file: $trackedFile")
    }
}

$decisionFiles = $repositoryFiles |
    Where-Object { $_ -match '^docs/decisions/[0-9]{4}-.*\.md$' -and $_ -notmatch '/0000-' }
$decisionIds = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($decisionFile in $decisionFiles) {
    $decisionId = [IO.Path]::GetFileName($decisionFile).Substring(0, 4)
    if (-not $decisionIds.Add($decisionId)) {
        $failures.Add("Duplicate ADR identifier: $decisionId")
    }
    $decisionText = Get-Content -LiteralPath (Join-Path $projectRoot $decisionFile) -Raw
    if ($decisionText -notmatch '(?m)^\*\*Status:\*\* (accepted|proposed|deprecated|rejected|superseded)\r?$') {
        $failures.Add("Invalid or missing ADR status metadata: $decisionFile")
    }
    if ($decisionText -notmatch '(?m)^\*\*Date:\*\* (?<date>[0-9]{4}-[0-9]{2}-[0-9]{2})\r?$') {
        $failures.Add("Invalid or missing ADR date metadata: $decisionFile")
    }
    else {
        try {
            [void][datetime]::ParseExact(
                $Matches['date'],
                'yyyy-MM-dd',
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::None
            )
        }
        catch {
            $failures.Add("Invalid ADR calendar date: $decisionFile")
        }
    }
    if ($decisionText -notmatch '(?m)^\*\*Decision scope:\*\* \S.+\r?$') {
        $failures.Add("Invalid or missing ADR decision-scope metadata: $decisionFile")
    }
    if ($decisionText -notmatch '(?m)^\*\*Supersedes:\*\* \S.+\r?$') {
        $failures.Add("Invalid or missing ADR supersedes metadata: $decisionFile")
    }
}

$strictUtf8 = [Text.UTF8Encoding]::new($false, $true)
$credentialPatterns = @(
    ('-----BEGIN ' + '(RSA |EC |OPENSSH )?PRIVATE KEY-----'),
    ('AK' + 'IA[0-9A-Z]{16}'),
    ('gh' + '[pousr]_[A-Za-z0-9]{30,}'),
    ('github' + '_pat_[A-Za-z0-9_]{40,}'),
    ('xox' + '[baprs]-[A-Za-z0-9-]{10,}'),
    ('A' + 'Iza[0-9A-Za-z_-]{35}'),
    ('s' + 'k-(?!image-)[A-Za-z0-9_-]{20,}')
)
$absoluteDeveloperPathPatterns = @(
    '[A-Za-z]:[\\/]Users[\\/][A-Za-z0-9._-]+(?:[\\/]|(?![A-Za-z0-9._-]))',
    '/(home|Users)/[A-Za-z0-9._-]+(?:/|(?![A-Za-z0-9._-]))'
)
foreach ($repositoryFile in $repositoryFiles) {
    $repositoryPath = Join-Path $projectRoot $repositoryFile
    try {
        $repositoryBytes = [IO.File]::ReadAllBytes($repositoryPath)
    }
    catch {
        $failures.Add("Unable to read repository file during content scan: $repositoryFile")
        continue
    }
    if ($repositoryBytes -contains 0) {
        continue
    }
    try {
        $content = $strictUtf8.GetString($repositoryBytes)
    }
    catch [Text.DecoderFallbackException] {
        continue
    }
    foreach ($pattern in $credentialPatterns) {
        if ($content -match $pattern) {
            $failures.Add("High-confidence credential material in repository file: $repositoryFile")
            break
        }
    }
    foreach ($pattern in $absoluteDeveloperPathPatterns) {
        if ($content -match $pattern) {
            $failures.Add("Absolute developer path in repository file: $repositoryFile")
            break
        }
    }
}

foreach ($generatedFile in $generatedFileInventory) {
    # An inventory entry must be a real artifact: tracked for the committed
    # tree, or present on disk while it awaits integration staging.
    $isTracked = $trackedFiles -contains $generatedFile
    $isPresent = Test-Path -LiteralPath (Join-Path $projectRoot $generatedFile) -PathType Leaf
    if (-not $isTracked -and -not $isPresent) {
        $failures.Add("Missing tracked generated-file inventory entry: $generatedFile")
    }
}

$mediaExtensions = @('.gif', '.ico', '.jpeg', '.jpg', '.mov', '.mp3', '.mp4', '.pdf', '.png', '.svg', '.wav', '.webm', '.webp')
$trackedMediaInventory = @()
$unlistedMedia = $trackedFiles | Where-Object {
    $mediaExtensions -contains [IO.Path]::GetExtension($_).ToLowerInvariant() -and
    $trackedMediaInventory -notcontains $_
}
foreach ($mediaFile in $unlistedMedia) {
    $failures.Add("Tracked media is missing from the explicit inventory: $mediaFile")
}

$requiredIgnoreRules = @(
    '__pycache__/', '.pytest_cache/', '.mypy_cache/', '.pyright/', '.ruff_cache/',
    '.hypothesis/', '.coverage', '.coverage.*', 'coverage.xml', 'htmlcov/', '.venv/', 'dist/', 'build/',
    '*.egg-info/', 'node_modules/', 'web/coverage/', 'web/playwright-report/',
    'web/test-results/', '.paritygrid/', 'data/', '*.db', '*.db-shm', '*.db-wal',
    '*.sqlite', '*.sqlite3', '*-shm', '*-wal', '*.duckdb', '*.parquet', '.env', '.env.*',
    '!.env.example', '*.pem', '*.key', '*.cer', '*.crt', '*.der', '*.kdbx', '*.p12',
    '*.pfx', '*.ppk', '*.tmp', '*.temp', '*.log'
)
$ignoreRules = Get-Content -LiteralPath (Join-Path $projectRoot '.gitignore') |
    ForEach-Object { $_.Trim() } |
    Where-Object { $_ -and -not $_.StartsWith('#') }
foreach ($requiredIgnoreRule in $requiredIgnoreRules) {
    if ($ignoreRules -notcontains $requiredIgnoreRule) {
        $failures.Add("Missing required .gitignore rule: $requiredIgnoreRule")
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

Write-Output "Instruction validation passed for $($markdownFiles.Count) Markdown files, $($decisionFiles.Count) ADRs, $($generatedFileInventory.Count) generated files, and $($trackedMediaInventory.Count) media files."

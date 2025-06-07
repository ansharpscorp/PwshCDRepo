Import-Module "$PSScriptRoot\Modules\GraphAuth.psm1"
Import-Module "$PSScriptRoot\Modules\Intervals.psm1"
Import-Module "$PSScriptRoot\Modules\Utils.psm1"

$config = Get-Content "$PSScriptRoot\config.json" | ConvertFrom-Json

Ensure-Directory $config.output_dir
Ensure-Directory (Split-Path $config.log_file)
Ensure-Directory (Split-Path $config.progress_file)

if (-Not (Test-Path $config.progress_file)) {
    "conference_id,status,retries,error" | Out-File $config.progress_file
}

$token = Get-AccessToken -TenantId $config.tenant_id -ClientId $config.client_id -ClientSecret $config.client_secret
$intervals = Get-DateIntervals -Date $config.date

foreach ($interval in $intervals) {
    Write-Host "Processing interval $($interval.Start) to $($interval.End)"
    & "$PSScriptRoot\Downloader.ps1" `
        -Start $interval.Start `
        -End $interval.End `
        -Token $token `
        -OutputDir $config.output_dir `
        -LogFile $config.log_file `
        -ProgressFile $config.progress_file `
        -MaxRetry $config.max_retry `
        -Backoff $config.retry_backoff
}

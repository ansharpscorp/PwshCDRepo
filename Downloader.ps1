param (
    $Start,
    $End,
    $Token,
    $OutputDir,
    $LogFile,
    $ProgressFile,
    $MaxRetry,
    $Backoff
)

Import-Module "$PSScriptRoot\Modules\Logger.psm1"
Import-Module "$PSScriptRoot\Modules\Utils.psm1"

function Get-ConferenceIds {
    param ($Start, $End, $Token)
    $ids = @()
    $url = "https://graph.microsoft.com/v1.0/communications/callRecords?\$filter=startDateTime ge $Start and endDateTime lt $End&\$top=999"
    do {
        $response = Invoke-RestMethod -Uri $url -Headers @{ Authorization = "Bearer $Token" } -Method GET
        $ids += $response.value.id
        $url = $response.'@odata.nextLink'
    } while ($url)
    return $ids
}

function Download-CallRecord {
    param ($ConfId, $Token)
    $url = "https://graph.microsoft.com/v1.0/communications/callRecords/$ConfId?\$expand=sessions,participants_v2"
    do {
        $response = Invoke-RestMethod -Uri $url -Headers @{ Authorization = "Bearer $Token" } -Method GET -ErrorAction Stop
        return $response
    } while ($false)
}

$ids = Get-ConferenceIds -Start $Start -End $End -Token $Token

$jobs = @()
$sem = [System.Threading.SemaphoreSlim]::new($Using:max_parallel_requests, $Using:max_parallel_requests)

foreach ($id in $ids) {
    $jobs += Start-ThreadJob -ScriptBlock {
        param ($ConfId, $Token, $OutDir, $Sem, $LogFile, $ProgressFile, $MaxRetry, $Backoff)

        $retry = 0
        $Sem.Wait()
        try {
            do {
                try {
                    $record = Download-CallRecord -ConfId $ConfId -Token $Token
                    Save-Json -Data $record -Path "$OutDir\$ConfId.json"
                    Write-Log -Message "SUCCESS: $ConfId" -LogFile $LogFile
                    Log-Progress -ConferenceId $ConfId -Status "success" -Retries $retry -Error "" -ProgressFile $ProgressFile
                    break
                } catch {
                    $retry++
                    if ($retry -ge $MaxRetry) {
                        Write-Log -Message "FAIL: $ConfId after $retry tries" -LogFile $LogFile
                        Log-Progress -ConferenceId $ConfId -Status "failure" -Retries $retry -Error $_.Exception.Message -ProgressFile $ProgressFile
                        break
                    } else {
                        Start-Sleep -Seconds $Backoff[$retry - 1]
                    }
                }
            } while ($true)
        } finally {
            $Sem.Release() | Out-Null
        }
    } -ArgumentList $id, $Token, $OutputDir, $sem, $LogFile, $ProgressFile, $MaxRetry, $Backoff
}

$jobs | Wait-Job | Receive-Job | Out-Null
$jobs | Remove-Job

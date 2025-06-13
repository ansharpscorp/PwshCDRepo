# Requires PowerShell 7+
# Required modules: MSAL.PS

param(
    [string]$ClientId,
    [string]$TenantId,
    [string]$ClientSecret,
    [string]$StartDate,
    [string]$InputCsvFile,
    [int]$MaxParallel = 5
)

$OutputFolder = "./Output_$StartDate"
$FailedCsvPath = "./Failed_$StartDate.csv"
$LogPath = "./Process_$StartDate.log"

$TokenInfo = @{
    Token = $null
    Expiry = (Get-Date).AddMinutes(-5)
}
$TokenLock = [System.Threading.Monitor]::new()
$LogLock = [System.Threading.Monitor]::new()
$CsvLock = [System.Threading.Monitor]::new()

function Get-AccessToken {
    $Body = @{ 
        client_id     = $ClientId
        scope         = 'https://graph.microsoft.com/.default'
        client_secret = $ClientSecret
        grant_type    = 'client_credentials'
    }
    $Response = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body $Body
    return $Response.access_token
}

function Get-ValidToken {
    [System.Threading.Monitor]::Enter($TokenLock)
    try {
        if ((Get-Date) -ge $TokenInfo.Expiry) {
            Write-Log "Refreshing access token..."
            $TokenInfo.Token = Get-AccessToken
            $TokenInfo.Expiry = (Get-Date).AddMinutes(50)
        }
        return $TokenInfo.Token
    }
    finally {
        [System.Threading.Monitor]::Exit($TokenLock)
    }
}

function Write-Log {
    param(
        [string]$Message
    )
    [System.Threading.Monitor]::Enter($LogLock)
    try {
        Add-Content -Path $LogPath -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') : $Message"
    }
    finally {
        [System.Threading.Monitor]::Exit($LogLock)
    }
}

function Write-FailedCsv {
    param(
        [string]$ConferenceId
    )
    [System.Threading.Monitor]::Enter($CsvLock)
    try {
        Add-Content -Path $FailedCsvPath -Value $ConferenceId
    }
    finally {
        [System.Threading.Monitor]::Exit($CsvLock)
    }
}

function Invoke-GraphCall {
    param(
        [string]$ConferenceId
    )
    $Token = Get-ValidToken
    $BaseUrl = "https://graph.microsoft.com/v1.0/communications/callRecords/$ConferenceId"

    try {
        $Response = Invoke-RestMethod -Uri $BaseUrl -Headers @{Authorization = "Bearer $Token"} -ErrorAction Stop

        # Get sessions with pagination
        $SessionsUrl = "$BaseUrl/sessions"
        $Sessions = @()
        do {
            $SessionsResponse = Invoke-RestMethod -Uri $SessionsUrl -Headers @{Authorization = "Bearer $Token"}
            $Sessions += $SessionsResponse.value
            $SessionsUrl = $SessionsResponse.'@odata.nextLink'
        } while ($SessionsUrl)

        # Get participants_v2 with pagination
        $ParticipantsUrl = "$BaseUrl/participants_v2"
        $Participants = @()
        do {
            $ParticipantsResponse = Invoke-RestMethod -Uri $ParticipantsUrl -Headers @{Authorization = "Bearer $Token"}
            $Participants += $ParticipantsResponse.value
            $ParticipantsUrl = $ParticipantsResponse.'@odata.nextLink'
        } while ($ParticipantsUrl)

        $FullRecord = [PSCustomObject]@{
            id              = $Response.id
            type            = $Response.type
            modalities      = $Response.modalities
            version         = $Response.version
            joinWebUrl      = $Response.joinWebUrl
            participants    = $Response.participants
            organizer       = $Response.organizer
            organizer_v2    = $Response.organizer_v2
            participants_v2 = $Participants
            sessions        = $Sessions
        }

        $OutPath = Join-Path $OutputFolder "$ConferenceId.json"

        # Check if file exists and compare size
        if (Test-Path $OutPath) {
            $OldSize = (Get-Item $OutPath).Length
            $NewSize = ($FullRecord | ConvertTo-Json -Depth 10).Length
            if ($NewSize -gt $OldSize) {
                $FullRecord | ConvertTo-Json -Depth 10 | Out-File -FilePath $OutPath -Force
                Write-Log "Overwrote file for: $ConferenceId (NewSize: $NewSize > OldSize: $OldSize)"
            } else {
                Write-Log "Skipped overwrite for: $ConferenceId (OldSize: $OldSize >= NewSize: $NewSize)"
            }
        } else {
            $FullRecord | ConvertTo-Json -Depth 10 | Out-File -FilePath $OutPath -Force
            Write-Log "Created new file for: $ConferenceId"
        }
    }
    catch {
        Write-Log "Failed: $ConferenceId - $_"
        Write-FailedCsv -ConferenceId $ConferenceId
    }
}

# Main Execution
if (-not (Test-Path $OutputFolder)) { New-Item -Path $OutputFolder -ItemType Directory }
$ConferenceIds = Import-Csv $InputCsvFile | Select-Object -ExpandProperty ConferenceId
$throttle = [System.Threading.SemaphoreSlim]::new($MaxParallel, $MaxParallel)
$Jobs = @()

Write-Log "Starting processing for date: $StartDate using file: $InputCsvFile"

foreach ($Id in $ConferenceIds) {
    $throttle.WaitAsync() | Out-Null
    $Jobs += [powershell]::Create().AddScript({
        param($ConfId)
        Invoke-GraphCall -ConferenceId $ConfId
        [System.Threading.SemaphoreSlim]::new($using:MaxParallel, $using:MaxParallel).Release() | Out-Null
    }).AddArgument($Id).BeginInvoke()
}

$Jobs | ForEach-Object { $_.AsyncWaitHandle.WaitOne() }
Write-Log "All jobs completed for date: $StartDate."

# Retry Script Function
function Retry-FailedCalls {
    Write-Log "Starting retry of failed calls..."
    $FailedIds = Get-Content $FailedCsvPath
    foreach ($FailId in $FailedIds) {
        Invoke-GraphCall -ConferenceId $FailId
    }
    Write-Log "Retry of failed calls completed."
}

# To run retry: Uncomment below line if retry is desired automatically
# Retry-FailedCalls

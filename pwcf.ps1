# Requires PowerShell 7+
# Required modules: MSAL.PS

param(
    [string]$ClientId,
    [string]$TenantId,
    [string]$ClientSecret,
    [string]$StartDate,
    [int]$StartHour,
    [int]$EndHour,
    [int]$MaxParallel = 5
)

$OutputFolder = "./Output_$StartDate"
$ConfIdCsvPath = "./ConfId_$StartDate.csv"
$LogPath = "./ConfIdProcess_$StartDate.log"

$TokenInfo = @{
    Token = $null
    Expiry = (Get-Date).AddMinutes(-5)
}
$TokenLock = [System.Threading.Monitor]::new()
$LogLock = [System.Threading.Monitor]::new()

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

function Fetch-ConferenceIds {
    param(
        [string]$Url
    )
    $Token = Get-ValidToken
    $ConfIds = @()
    try {
        do {
            $Response = Invoke-RestMethod -Uri $Url -Headers @{Authorization = "Bearer $Token"}
            $ConfIds += $Response.value | Select-Object -ExpandProperty id
            $Url = $Response.'@odata.nextLink'
        } while ($Url)
    }
    catch {
        Write-Log "Error fetching Conference IDs: $_"
    }
    return $ConfIds
}

# Main Execution
if (-not (Test-Path $OutputFolder)) { New-Item -Path $OutputFolder -ItemType Directory }
$throttle = [System.Threading.SemaphoreSlim]::new($MaxParallel, $MaxParallel)
$Jobs = @()
$AllConfIds = @()

Write-Log "Starting Conference ID fetching for date: $StartDate"

for ($hour = $StartHour; $hour -le $EndHour; $hour++) {
    for ($min = 0; $min -lt 60; $min += 15) {
        $StartTime = "${StartDate}T{0:D2}:{1:D2}:00Z" -f $hour, $min
        $EndMinute = $min + 15
        $EndHourTemp = $hour
        if ($EndMinute -ge 60) {
            $EndMinute = 0
            $EndHourTemp++
        }
        $EndTime = "${StartDate}T{0:D2}:{1:D2}:00Z" -f $EndHourTemp, $EndMinute

        $GraphUrl = "https://graph.microsoft.com/v1.0/communications/callRecords?`$filter=startDateTime ge $StartTime and startDateTime lt $EndTime"

        $throttle.WaitAsync() | Out-Null
        $Jobs += [powershell]::Create().AddScript({
            param($Url)
            Fetch-ConferenceIds -Url $Url
            [System.Threading.SemaphoreSlim]::new($using:MaxParallel, $using:MaxParallel).Release() | Out-Null
        }).AddArgument($GraphUrl).BeginInvoke()
    }
}

$Jobs | ForEach-Object { $_.AsyncWaitHandle.WaitOne() }
$Jobs | ForEach-Object {
    $Result = $_.EndInvoke($_)
    if ($Result) { $AllConfIds += $Result }
}

$AllConfIds | Sort-Object | Get-Unique | ForEach-Object { [PSCustomObject]@{ ConferenceId = $_ } } | Export-Csv -Path $ConfIdCsvPath -NoTypeInformation
Write-Log "Conference IDs saved to $ConfIdCsvPath"
Write-Log "Conference ID fetching completed for date: $StartDate."

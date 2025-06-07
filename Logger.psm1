function Write-Log {
    param ($Message, $LogFile)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$timestamp - $Message" | Out-File -Append -FilePath $LogFile
}

function Log-Progress {
    param (
        [string]$ConferenceId,
        [string]$Status,
        [int]$Retries,
        [string]$Error,
        [string]$ProgressFile
    )
    $row = "$ConferenceId,$Status,$Retries,""$Error"""
    Add-Content -Path $ProgressFile -Value $row
}
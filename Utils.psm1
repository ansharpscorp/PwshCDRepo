function Ensure-Directory {
    param ($Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Save-Json {
    param ($Data, $Path)
    $Data | ConvertTo-Json -Depth 10 | Out-File -FilePath $Path -Encoding utf8
}
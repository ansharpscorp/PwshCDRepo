function Get-DateIntervals {
    param ([string]$Date)
    $base = [datetime]::Parse($Date)
    $intervals = @()
    for ($i = 0; $i -lt 24 * 4; $i++) {
        $start = $base.AddMinutes(15 * $i).ToString("o")
        $end = $base.AddMinutes(15 * ($i + 1)).ToString("o")
        $intervals += [PSCustomObject]@{ Start = $start; End = $end }
    }
    return $intervals
}
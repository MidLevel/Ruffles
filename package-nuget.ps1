$version;
if (Test-Path env:SEMANTIC_NEXT_VERSION)
{
    $version = "$env:SEMANTIC_NEXT_VERSION"
}
elseif (Test-Path env:SEMANTIC_LAST_VERSION)
{
    $version = "$env:SEMANTIC_LAST_VERSION"
}
else
{
    $version = "0.1.0"
}

$releaseNoteUrl = -join ("https://github.com/MidLevel/Ruffles/releases/tag/v", $version)

$args = -join("pack Ruffles.nuspec -Version ", $version, " -Properties Configuration=Release;releaseNotes=", $releaseNoteUrl)

Start-Process -FilePath "nuget" -ArgumentList $args
# Download URL for Tabular Editor portable:
$TabularEditorUrl = "https://cdn.tabulareditor.com/files/TabularEditor.2.26.0.zip" 

# Download destination (root of PowerShell script execution path):
$DownloadDestination = join-path (get-location) "TabularEditor.zip"

Write-Output "Downloading Tabular Editor from $TabularEditorUrl to $DownloadDestination"

# Download from GitHub:
Invoke-WebRequest -Uri $TabularEditorUrl -OutFile $DownloadDestination

# Unzip Tabular Editor portable, and then delete the zip file:
Expand-Archive -Path $DownloadDestination -DestinationPath (get-location).Path
Remove-Item $DownloadDestination
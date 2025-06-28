for ($i=21; $i -le 30; $i++) {
    $batchNumber = $i
    $filePath = "HR_tickets_JSON_response_20250624_134239\hr_records_batch_${batchNumber}.json"
    $command = "py HR_ticket_handling_to_download_attachments.py $filePath"
    Start-Process powershell.exe -ArgumentList "-NoExit", "-Command", $command
    Start-Sleep -Seconds 5
}

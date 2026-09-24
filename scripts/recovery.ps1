#requires -Version 7.0
[CmdletBinding()]
param([Parameter(Mandatory)][string]$Directory, [ValidateRange(60,1800)][int]$TimeoutSeconds=300)
. "$PSScriptRoot/common.ps1"
New-Item -ItemType Directory -Force -Path $Directory | Out-Null
$probeId=[guid]::NewGuid().ToString()
$orderId=[guid]::NewGuid().ToString()
$job='probe-recovery-'+[guid]::NewGuid().ToString('N').Substring(0,12)
$topic='observa.probe.started.v1'
$group='observa-spike0-transform'
$armed=$false

function New-RecoveryJob {
    $common=@{
        image='observa-probe-python:spike0';imagePullPolicy='Never'
        env=@(@{name='KAFKA_BOOTSTRAP_SERVERS';value='kafka:9092'},@{name='OTEL_EXPORTER_OTLP_ENDPOINT';value='http://collector:4318'})
        volumeMounts=@(@{name='carrier';mountPath='/work'})
        resources=@{requests=@{cpu='50m';memory='64Mi'};limits=@{cpu='250m';memory='192Mi'}}
        securityContext=@{allowPrivilegeEscalation=$false;capabilities=@{drop=@('ALL')}}
    }
    $entry=$common.Clone();$entry.name='probe-python-entry'
    $entry.command=@('python','-m','observa_probe','entry','--directory','/work','--count','1','--order-id',$orderId,'--probe-id',$probeId)
    $relay=$common.Clone();$relay.name='probe-python';$relay.command=@('python','-m','observa_probe','relay','--directory','/work')
    return @{
        apiVersion='batch/v1';kind='Job';metadata=@{name=$job;namespace=$script:Namespace}
        spec=@{backoffLimit=0;activeDeadlineSeconds=180;template=@{
            metadata=@{labels=@{'app.kubernetes.io/part-of'='observa-spike0';'app.kubernetes.io/component'='probe'}}
            spec=@{restartPolicy='Never';automountServiceAccountToken=$false;securityContext=@{runAsNonRoot=$true;runAsUser=10001;fsGroup=10001};volumes=@(@{name='carrier';emptyDir=@{}});initContainers=@($entry);containers=@($relay)}
        }}
    }
}

function Read-ProbeRecords {
    param([string]$App,[string]$Phase)
    $pods=Invoke-Kube @('get','pods','-l',"app=$App",'-o','json') 30 | ConvertFrom-Json
    foreach($pod in $pods.items){
        foreach($previous in @($false,$true)){
            $arguments=@('logs',$pod.metadata.name,'--tail=3000')
            if($previous){$arguments+= '--previous'}
            try {
                $text=Invoke-Kube $arguments 15
                $text | Set-Content (Join-Path $Directory "$Phase-$($pod.metadata.name)-previous-$previous.jsonl")
                foreach($line in ($text -split '\r?\n')){
                    if($line.StartsWith('{')){
                        try { $record=$line | ConvertFrom-Json -AsHashtable; if($record.probe_id -eq $probeId){$record} }
                        catch { Write-Verbose 'Ignoring a non-JSON container log line.' }
                    }
                }
            } catch { Write-Verbose "Logs unavailable for $($pod.metadata.name), previous=$previous : $_" }
        }
    }
}

function Get-CommittedOffset {
    param([int]$Partition,[string]$Phase)
    $raw=Invoke-Kube @('exec','kafka-0','--','/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','kafka:9092','--group',$group,'--describe') 30
    $raw | Set-Content (Join-Path $Directory "$Phase-consumer-group.txt")
    # Match the stable leading columns, ignoring optional consumer/client columns.
    foreach($line in ($raw -split '\r?\n')){
        $columns=$line.Trim() -split '\s+'
        if($columns.Count -ge 4 -and $columns[0] -eq $group -and $columns[1] -eq $topic -and $columns[2] -eq "$Partition"){
            if($columns[3] -eq '-'){return [long]-1}
            $offset=[long]0
            if([long]::TryParse($columns[3],[ref]$offset)){return $offset}
            throw "Malformed committed offset in consumer group output: $line"
        }
    }
    throw "Missing group/partition row for $topic partition $Partition; cannot prove commit state."
}

try {
    $deployment=Invoke-Kube @('get','deployment/probe-node','-o','json') 30 | ConvertFrom-Json -AsHashtable
    $existing=@($deployment.spec.template.spec.containers | ForEach-Object { $_.env } | Where-Object {$_.name -eq 'FAIL_BEFORE_PUBLISH_PROBE_ID'})
    if($existing.Count){throw 'An existing fault setting is present; refusing to overwrite it.'}
    Save-Json @{probe_id=$probeId;order_id=$orderId;job=$job;started=[DateTimeOffset]::UtcNow.ToString('o')} (Join-Path $Directory 'recovery-input.json')
    $armed=$true
    Invoke-Kube @('set','env','deployment/probe-node',"FAIL_BEFORE_PUBLISH_PROBE_ID=$probeId") 30 | Write-Host
    Invoke-Kube @('rollout','status','deployment/probe-node',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
    $jobPath=Join-Path $Directory "$job.json"
    Save-Json (New-RecoveryJob) $jobPath
    Invoke-Kube @('apply','-f',$jobPath) 30 | Write-Host
    Invoke-Kube @('wait','--for=condition=complete',"job/$job",'--timeout=180s') 190 | Write-Host
    $relayText=Invoke-Kube @('logs',"job/$job",'-c','probe-python') 30
    $relayText | Set-Content (Join-Path $Directory 'recovery-relay.jsonl')
    $published=@(foreach($line in ($relayText -split '\r?\n')){
        if($line.StartsWith('{')){$record=$line | ConvertFrom-Json -AsHashtable;if($record.event -eq 'published' -and $record.probe_id -eq $probeId -and $record.topic -eq $topic){$record}}
    })
    if($published.Count -ne 1){throw 'Expected exactly one acknowledged fault-probe publish.'}
    $partition=[int]$published[0].partition;$messageOffset=[long]$published[0].offset
    $deadline=[DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $fault=@()
    do {
        $fault=@(Read-ProbeRecords 'probe-node' 'fault' | Where-Object {$_.event -eq 'process_error' -and $_.error -like '*fault_before_publish*' -and [int]$_.partition -eq $partition -and [long]$_.offset -eq $messageOffset})
        if($fault.Count){break}
        Start-Sleep -Seconds 2
    } while([DateTime]::UtcNow -lt $deadline)
    if(-not $fault.Count){throw 'Timed out without observing the intended pre-publish processing failure.'}
    $before=Get-CommittedOffset $partition 'fault'
    if($before -gt $messageOffset){throw "Faulty message was committed: current=$before message=$messageOffset"}
    Save-Json @{probe_id=$probeId;partition=$partition;message_offset=$messageOffset;committed_offset=$before;fault=$fault} (Join-Path $Directory 'recovery-before.json')
} finally {
    if($armed){
        Invoke-Kube @('set','env','deployment/probe-node','FAIL_BEFORE_PUBLISH_PROBE_ID-') 30 | Write-Host
        Invoke-Kube @('rollout','status','deployment/probe-node',"--timeout=${TimeoutSeconds}s") ($TimeoutSeconds+10) | Write-Host
    }
}

$deadline=[DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
$recovered=@();$sink=@();$after=[long]-1
 do {
    $recovered=@(Read-ProbeRecords 'probe-node' 'recovered' | Where-Object {$_.event -eq 'process_end' -and $_.order_id -eq $orderId -and $_.topic -eq $topic -and [int]$_.partition -eq $partition -and [long]$_.offset -eq $messageOffset})
    $sink=@(Read-ProbeRecords 'probe-python-sink' 'recovered' | Where-Object {$_.event -eq 'process_end' -and $_.order_id -eq $orderId})
    if($recovered.Count -and $sink.Count){$after=Get-CommittedOffset $partition 'recovered';if($after -gt $messageOffset){break}}
    Start-Sleep -Seconds 2
} while([DateTime]::UtcNow -lt $deadline)
if(-not $recovered.Count -or -not $sink.Count -or $after -le $messageOffset){throw 'Recovery was not proven: require same input offset processed again, sink success, and advanced commit.'}
Save-Json @{passed=$true;probe_id=$probeId;order_id=$orderId;partition=$partition;message_offset=$messageOffset;committed_before=$before;committed_after=$after;node=$recovered;sink=$sink;completed=[DateTimeOffset]::UtcNow.ToString('o')} (Join-Path $Directory 'recovery-verification.json')
Write-Host 'Controlled fault recovery verified: no early commit, same input redelivered, sink completed, commit advanced.'

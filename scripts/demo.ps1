#requires -Version 7.0
[CmdletBinding()]
param([Parameter(Mandatory)][string]$Directory, [int]$TimeoutSeconds=600)
. "$PSScriptRoot/common.ps1"
$started=[DateTimeOffset]::UtcNow
$forwards=@()
$entries=@()
$jobs=@()
$isolations=@()
$primaryOrder=[guid]::NewGuid().ToString()

function New-ProbeJob {
    param([string]$Name,[string]$OrderId)
    $common=@{
        image='observa-probe-python:spike0';imagePullPolicy='Never'
        env=@(@{name='KAFKA_BOOTSTRAP_SERVERS';value='kafka:9092'},@{name='OTEL_EXPORTER_OTLP_ENDPOINT';value='http://collector:4318'})
        volumeMounts=@(@{name='carrier';mountPath='/work'})
        resources=@{requests=@{cpu='50m';memory='64Mi'};limits=@{cpu='250m';memory='192Mi'}}
        securityContext=@{allowPrivilegeEscalation=$false;capabilities=@{drop=@('ALL')}}
    }
    $entry=$common.Clone();$entry.name='probe-python-entry';$entry.command=@('python','-m','observa_probe','entry','--directory','/work','--count','5','--order-id',$OrderId)
    $relay=$common.Clone();$relay.name='probe-python';$relay.command=@('python','-m','observa_probe','relay','--directory','/work')
    return @{
        apiVersion='batch/v1';kind='Job';metadata=@{name=$Name;namespace=$script:Namespace}
        spec=@{backoffLimit=0;activeDeadlineSeconds=180;template=@{
            metadata=@{labels=@{'app.kubernetes.io/part-of'='observa-spike0';'app.kubernetes.io/component'='probe'}}
            spec=@{restartPolicy='Never';automountServiceAccountToken=$false;securityContext=@{runAsNonRoot=$true;runAsUser=10001;fsGroup=10001};volumes=@(@{name='carrier';emptyDir=@{}});initContainers=@($entry);containers=@($relay)}
        }}
    }
}

try {
    foreach($forward in @(@('tempo',13200,3200),@('loki',13100,3100),@('grafana',13000,3000),@('prometheus',19090,9090))){
        $forwards+=Start-LocalForward $forward[0] $forward[1] $forward[2]
    }
    $null=Wait-Http 'http://127.0.0.1:13200/ready'
    $null=Wait-Http 'http://127.0.0.1:13100/ready'
    $null=Wait-Http 'http://127.0.0.1:13000/api/health'
    $null=Wait-Http 'http://127.0.0.1:19090/-/ready'
    # Keep the first order as the acceptance fixture. Additional orders are only
    # used to prove that the two-partition topic is exercised by this run.
    $partitions=@()
    for($attempt=0;$attempt -lt 12 -and @($partitions | Sort-Object -Unique).Count -lt 2;$attempt++){
        $order=if($attempt -eq 0){$primaryOrder}else{[guid]::NewGuid().ToString()}
        $job='probe-demo-'+[guid]::NewGuid().ToString('N').Substring(0,12)
        $jobs+=$job
        $path=Join-Path $Directory "$job.json"
        Save-Json (New-ProbeJob $job $order) $path
        Invoke-Kube @('apply','-f',$path) | Write-Host
        Invoke-Kube @('wait','--for=condition=complete',"job/$job",'--timeout=180s') 190 | Write-Host
        $podList=Invoke-Kube @('get','pods','-l',"job-name=$job",'-o','json') 30 | ConvertFrom-Json -AsHashtable
        $pod=$podList.items[0]
        $init=$pod.status.initContainerStatuses[0]
        $relayStatus=$pod.status.containerStatuses[0]
        if($init.state.terminated.exitCode -ne 0 -or $relayStatus.state.terminated.exitCode -ne 0 -or
           [DateTimeOffset]$init.state.terminated.finishedAt -gt [DateTimeOffset]$relayStatus.state.terminated.startedAt -or
           $init.containerID -eq $relayStatus.containerID){throw 'Entry/relay process isolation was not established.'}
        $isolations+=@{job=$job;entry_container=$init.containerID;relay_container=$relayStatus.containerID;entry_finished=$init.state.terminated.finishedAt;relay_started=$relayStatus.state.terminated.startedAt}
        Save-Json $isolations (Join-Path $Directory 'process-isolation.json')
        $entryText=Invoke-Kube @('logs',"job/$job",'-c','probe-python-entry') 30
        $relayText=Invoke-Kube @('logs',"job/$job",'-c','probe-python') 30
        $entryText | Set-Content (Join-Path $Directory "$job-entry.jsonl")
        $relayText | Set-Content (Join-Path $Directory "$job-relay.jsonl")
        foreach($line in ($entryText -split '\r?\n')){
            if(-not $line.StartsWith('{')){continue}
            $record=$line | ConvertFrom-Json
            if($record.event -eq 'entry_manifest'){$entries+=@($record.probes)}
        }
        foreach($line in ($relayText -split '\r?\n')){
            if(-not $line.StartsWith('{')){continue}
            $record=$line | ConvertFrom-Json
            if($record.event -eq 'published'){$partitions+=$record.partition}
        }
        Save-ClusterDiagnostics $Directory
    }
    if(@($partitions | Sort-Object -Unique).Count -lt 2){throw 'Did not observe delivery to both Kafka partitions within 12 keys.'}
    if($entries.Count -lt 5){throw 'Missing entry manifests; cannot verify traces.'}
    $primary=@($entries | Where-Object {$_.order_id -eq $primaryOrder} | Sort-Object file)
    if($primary.Count -ne 5){throw "Primary order $primaryOrder must have exactly five probes; observed $($primary.Count)."}
    $manifest=@{probe_ids=@($primary.probe_id);order_id=$primaryOrder;topics=@('observa.probe.started.v1','observa.probe.completed.v1');expected_tracestate='observa=spike0';probes=$entries;partition_samples=@($partitions | Sort-Object -Unique)}
    Save-Json $manifest (Join-Path $Directory 'manifest.json')
    $deadline=[DateTime]::UtcNow.AddSeconds([Math]::Min($TimeoutSeconds,180))
    do {
        $traces=@()
        foreach($traceId in @($entries.trace_id | Sort-Object -Unique)){
            try {$traces+=Invoke-RestMethod "http://127.0.0.1:13200/api/traces/$traceId" -Headers @{Accept='application/json'} -TimeoutSec 5}
            catch {Write-Verbose "Waiting for trace $traceId"}
        }
        Save-Json @($traces) (Join-Path $Directory 'traces.json')
        $query=[Uri]::EscapeDataString('{k8s_namespace_name="observa-spike0"}')
        $since=$started.ToUnixTimeMilliseconds().ToString()+'000000'
        $loki=Invoke-RestMethod "http://127.0.0.1:13100/loki/api/v1/query_range?query=$query&start=$since&limit=5000&direction=forward" -Headers @{'X-Loki-Response-Encoding-Flags'='categorize-labels'} -TimeoutSec 10
        Save-Json $loki (Join-Path $Directory 'loki.json')
        try {
            $result=Invoke-Checked docker @('run','--rm','--mount',"type=bind,source=$script:Root,target=/workspace,readonly",'--workdir','/workspace','--entrypoint','python','observa-probe-python:spike0','tests/evidence/verify.py','--directory',('/workspace/'+[IO.Path]::GetRelativePath($script:Root,$Directory).Replace('\','/'))) 60
            $result | Set-Content (Join-Path $Directory 'verification.json')
            Write-Host $result
            $verified=$true
            break
        } catch { $_.Exception.Message | Set-Content (Join-Path $Directory 'verification-pending.txt'); Start-Sleep -Seconds 3 }
    } while([DateTime]::UtcNow -lt $deadline)
    if(-not(Get-Variable verified -ErrorAction SilentlyContinue)){throw 'Evidence verification failed; inspect verification-pending.txt. No acceptance claimed.'}
    $metricsDeadline=[DateTime]::UtcNow.AddSeconds(60)
    do {
        $targets=Invoke-RestMethod 'http://127.0.0.1:19090/api/v1/targets' -TimeoutSec 10
        $required=@($targets.data.activeTargets | Where-Object {$_.labels.job -in @('collector','probes')})
        $healthy=@($required | Where-Object {$_.health -eq 'up'})
        if($required.Count -ge 4 -and $healthy.Count -eq $required.Count){break}
        Start-Sleep -Seconds 3
    } while([DateTime]::UtcNow -lt $metricsDeadline)
    Save-Json $targets (Join-Path $Directory 'prometheus-targets.json')
    if($required.Count -lt 4 -or $healthy.Count -ne $required.Count){throw 'Collector and all three probes must be scraped successfully.'}
    $metricQuery=[Uri]::EscapeDataString('probe_processed_total')
    Save-Json (Invoke-RestMethod "http://127.0.0.1:19090/api/v1/query?query=$metricQuery" -TimeoutSec 10) (Join-Path $Directory 'prometheus-counters.json')
    Save-ClusterDiagnostics $Directory
    Write-Host 'API evidence verified. Real Grafana navigation and failure/rebuild tests remain separate acceptance gates.'
} finally {
    foreach($forward in $forwards){if(-not $forward.HasExited){$forward.Kill($true)};$forward.Dispose()}
}



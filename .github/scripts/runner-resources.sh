#!/usr/bin/env bash
# Benchmark helper: `info` prints the runner hardware, `start` samples CPU and
# memory use every 2s in the background, `report` summarises the samples.
set -u
samples="${RUNNER_TEMP:-/tmp}/runner-resources.csv"

meminfo() { awk -v k="$1" '$1 == k":" {print int($2 / 1024)}' /proc/meminfo; }

case "$1" in
  info)
    echo "vCPUs: $(nproc)"
    echo "CPU: $(awk -F': ' '/^model name/ {print $2; exit}' /proc/cpuinfo)"
    echo "CPU MHz: $(awk -F': ' '/^cpu MHz/ {print $2; exit}' /proc/cpuinfo)"
    echo "Memory: $(meminfo MemTotal) MiB"
    df -h "$GITHUB_WORKSPACE" | tail -1 | awk '{print "Workspace disk: " $2 " total, " $4 " free (" $1 ")"}'
    if [[ "$RUNNER_OS" == "Windows" ]]; then
      powershell -NoProfile -Command "Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors, MaxClockSpeed | Format-List; Get-CimInstance Win32_ComputerSystem | Select-Object Manufacturer, Model | Format-List; Get-PhysicalDisk | Select-Object FriendlyName, MediaType, BusType, Size | Format-List"
    else
      echo "Virtualization: $(systemd-detect-virt 2>/dev/null || echo unknown)"
      echo "DMI product: $(cat /sys/class/dmi/id/product_name 2>/dev/null || echo unknown)"
      token=$(curl -sf -m 1 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)
      echo "EC2 instance type: $(curl -sf -m 1 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-type || echo n/a)"
      lsblk -d -o NAME,SIZE,ROTA,MODEL 2>/dev/null || true
    fi
    ;;
  start)
    echo "seconds,cpu_busy_pct,mem_used_mib" > "$samples"
    nohup bash -c '
      read -r _ u n s i w q sq st _ < /proc/stat; prev_busy=$((u+n+s+q+sq+st)); prev_total=$((prev_busy+i+w)); t0=$SECONDS
      total_mem=$(awk "/^MemTotal:/ {print \$2}" /proc/meminfo)
      while sleep 2; do
        read -r _ u n s i w q sq st _ < /proc/stat; busy=$((u+n+s+q+sq+st)); total=$((busy+i+w))
        avail=$(awk "/^MemAvailable:/ {a=\$2} /^MemFree:/ {f=\$2} END {print a ? a : f}" /proc/meminfo)
        echo "$((SECONDS-t0)),$(( (busy-prev_busy)*100 / (total-prev_total > 0 ? total-prev_total : 1) )),$(( (total_mem-avail)/1024 ))"
        prev_busy=$busy; prev_total=$total
      done >> '"$samples"'
    ' > /dev/null 2>&1 &
    ;;
  report)
    awk -F, 'NR > 1 {n++; cpu += $2; if ($2 > maxcpu) maxcpu = $2; if ($3 > maxmem) maxmem = $3; mem += $3}
      END {if (n) printf "RESOURCES samples=%d avg_cpu=%.0f%% peak_cpu=%d%% avg_mem=%.0fMiB peak_mem=%dMiB\n", n, cpu/n, maxcpu, mem/n, maxmem}' "$samples"
    ;;
esac

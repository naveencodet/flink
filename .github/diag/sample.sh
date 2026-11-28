#!/usr/bin/env bash
# Samples CPU / memory / disk once per second into a CSV. Usage: sample.sh <out.csv>
out="$1"
echo "ts,load1,cpu_user,cpu_sys,cpu_idle,cpu_iowait,mem_used_mb,mem_avail_mb,disk_used_pct,disk_free_gb" > "$out"
while true; do
  read -r _ u n s i w _ < <(grep '^cpu ' /proc/stat); t1=$((u+n+s+i+w)); i1=$i; u1=$((u+n)); s1=$s; w1=$w
  sleep 1
  read -r _ u n s i w _ < <(grep '^cpu ' /proc/stat); t2=$((u+n+s+i+w)); d=$((t2-t1)); [ "$d" -eq 0 ] && d=1
  echo "$(date +%s.%N),$(cut -d' ' -f1 /proc/loadavg),$((100*(u+n-u1)/d)),$((100*(s-s1)/d)),$((100*(i-i1)/d)),$((100*(w-w1)/d)),$(free -m | awk '/Mem:/{print $3","$7}'),$(df --output=pcent / | tail -1 | tr -dc 0-9),$(df -BG --output=avail / | tail -1 | tr -dc 0-9)" >> "$out"
done

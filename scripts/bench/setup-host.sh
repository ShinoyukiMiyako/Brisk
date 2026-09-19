#!/usr/bin/env bash
# Reports, and with --apply sets, the kernel and CPU settings the Brisk
# benchmarks expect on the Linux benchmark host.
#
# Usage: scripts/bench/setup-host.sh [--check | --apply [--deploy-tips] [--persist] | --restore]
#
#   (no flag)      report only; changes nothing
#   --check        report only; exit status 3 if any setting is marked [ ]
#                  (run-m0.sh gates a session on this)
#   --apply        set the benchmark sysctls, the performance governor and
#                  boost off (the last two only where cpufreq is exposed)
#   --deploy-tips  with --apply: also set fq and bbr, the deployment tips of
#                  the design document. They do not act on loopback (lo has no
#                  qdisc) and bbr's internal pacing would add its own timing to
#                  loopback benchmarks, so they are off unless asked for.
#   --persist      with --apply: also write /etc/sysctl.d/90-brisk-bench.conf
#   --restore      put back the values found before the first --apply and
#                  remove the file written by --persist
#
# Changes are runtime-only unless --persist is given; a reboot undoes them.
# The values found before a change are kept in /run/brisk-bench/ (cleared by
# a reboot, like the changes themselves) and, with --persist, also in
# /var/lib/brisk-bench/. --apply also prints the equivalent rollback commands.
# Never reboots and never touches network interfaces.

set -euo pipefail

usage() {
    sed -n '2,24p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

apply=0
check=0
deploy_tips=0
persist=0
restore=0
while (($# > 0)); do
    case "$1" in
        --apply) apply=1 ;;
        --check) check=1 ;;
        --deploy-tips) deploy_tips=1 ;;
        --persist) persist=1 ;;
        --restore) restore=1 ;;
        -h | --help) usage; exit 0 ;;
        *) echo "setup-host: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done
if ((restore && (apply || check || deploy_tips || persist))); then
    echo "setup-host: --restore takes no other flag" >&2
    exit 2
fi
if ((check && apply)); then
    echo "setup-host: --check and --apply exclude each other" >&2
    exit 2
fi
if (((deploy_tips || persist) && !apply)); then
    echo "setup-host: --deploy-tips and --persist need --apply" >&2
    exit 2
fi
if [[ "$(uname -s)" != Linux ]]; then
    echo "setup-host: the benchmark host must run Linux" >&2
    exit 2
fi

# Fixed root-owned paths: under `sudo` HOME and XDG_STATE_HOME point
# elsewhere, so a per-user path would hide the saved values from a later
# --restore. /run is a tmpfs, so runtime-only originals vanish with the
# runtime-only changes they belong to.
runtime_state=/run/brisk-bench/host-before.tsv
persist_state=/var/lib/brisk-bench/host-before.tsv
persist_file=/etc/sysctl.d/90-brisk-bench.conf
persist_marker="# Written by scripts/bench/setup-host.sh --persist"

# Design document, "deployment": somaxconn=4096, tcp_slow_start_after_idle=0,
# tcp_notsent_lowat=131072. The SYN backlog matches the listeners' backlog of
# 4096 so the burst of pre-built streams at start is not throttled.
bench_sysctls=(
    "net.core.somaxconn=4096"
    "net.ipv4.tcp_max_syn_backlog=4096"
    "net.ipv4.tcp_slow_start_after_idle=0"
    "net.ipv4.tcp_notsent_lowat=131072"
)
deploy_sysctls=(
    "net.core.default_qdisc=fq"
    "net.ipv4.tcp_congestion_control=bbr"
)

changed=0
unmet=0
rollback=()

as_root() {
    if ((EUID == 0)); then
        "$@"
    else
        sudo -n "$@"
    fi
}

# sysctl prints multi-value keys tab-separated; compare on single spaces.
normalize() {
    tr -s '[:space:]' ' ' <<<"$1" | sed 's/^ //; s/ $//'
}

read_sysctl() {
    normalize "$(sysctl -n "$1" 2>/dev/null || echo unavailable)"
}

# Kernel files that some hosts (virtual machines in particular) lack.
read_file() {
    if [[ -r "$1" ]]; then
        cat -- "$1"
    else
        echo unavailable
    fi
}

# Appends an entry to a state file unless the key is already recorded, so
# repeated --apply runs never overwrite the original value.
remember_in() {
    local file="$1" kind="$2" key="$3" value="$4"
    if [[ -f "$file" ]] && grep -q -F -- "$(printf '%s\t%s\t' "$kind" "$key")" "$file"; then
        return 0
    fi
    as_root install -d -m 0755 "${file%/*}"
    printf '%s\t%s\t%s\n' "$kind" "$key" "$value" | as_root tee -a "$file" >/dev/null
}

remember() {
    local kind="$1" key="$2" value="$3"
    remember_in "$runtime_state" "$kind" "$key" "$value"
    case "$kind" in
        sysctl) rollback+=("sudo sysctl -w $key='$value'") ;;
        sysfs) rollback+=("echo '$value' | sudo tee $key") ;;
    esac
}

ensure_sysctl() {
    local key="${1%%=*}" want
    want="$(normalize "${1#*=}")"
    local have
    have="$(read_sysctl "$key")"
    if [[ "$have" == "$want" ]]; then
        printf '  [x] %-42s %s\n' "$key" "$have"
    elif ((apply)); then
        remember sysctl "$key" "$have"
        as_root sysctl -q -w "$key=$want"
        printf '  [x] %-42s %s (changed from %s)\n' "$key" "$(read_sysctl "$key")" "$have"
        changed=$((changed + 1))
    else
        printf '  [ ] %-42s %s (want %s)\n' "$key" "$have" "$want"
        unmet=$((unmet + 1))
    fi
}

report_sysctl() {
    printf '      %-42s %s\n' "$1" "$(read_sysctl "$1")"
}

# Checks, and with --apply sets, the first of the given sysfs files that
# exists; a label stands in for the file when none does.
ensure_sysfs() {
    local want="$1" label="$2"
    shift 2
    local file have
    for file in "$@"; do
        [[ -e "$file" ]] || continue
        have="$(<"$file")"
        if [[ "$have" == "$want" ]]; then
            printf '  [x] %-42s %s\n' "${file#/sys/devices/system/}" "$have"
        elif ((apply)); then
            remember sysfs "$file" "$have"
            as_root tee "$file" >/dev/null <<<"$want"
            printf '  [x] %-42s %s (changed from %s)\n' "${file#/sys/devices/system/}" "$(<"$file")" "$have"
            changed=$((changed + 1))
        else
            printf '  [ ] %-42s %s (want %s)\n' "${file#/sys/devices/system/}" "$have" "$want"
            unmet=$((unmet + 1))
        fi
        return 0
    done
    printf '      %-42s not exposed on this host, skipped\n' "$label"
}

restore_from() {
    local file="$1" kind key value
    [[ -f "$file" ]] || return 0
    while IFS=$'\t' read -r kind key value; do
        case "$kind" in
            sysctl) as_root sysctl -q -w "$key=$value" ;;
            sysfs) as_root tee "$key" >/dev/null <<<"$value" ;;
            *) echo "setup-host: unknown entry kind '$kind' in $file" >&2; exit 1 ;;
        esac
        printf '  restored %s = %s (from %s)\n' "$key" "$value" "$file"
    done <"$file"
}

do_restore() {
    local found=0
    if [[ -f "$persist_file" ]]; then
        if [[ "$(head -n 1 "$persist_file")" == "$persist_marker" ]]; then
            as_root rm -f "$persist_file"
            echo "  removed $persist_file"
        else
            echo "  note: $persist_file was not written by this script; left in place"
        fi
    fi
    # The runtime file first: for a key in both, the persisted file holds the
    # value from before the persisted settings, the true original.
    for file in "$runtime_state" "$persist_state"; do
        if [[ -f "$file" ]]; then
            found=1
            restore_from "$file"
            as_root rm -f "$file"
        fi
    done
    if ((!found)); then
        echo "setup-host: nothing to restore ($runtime_state and $persist_state do not exist)"
    fi
}

if ((restore)); then
    do_restore
    exit 0
fi

echo "== host"
printf '      %-42s %s\n' hostname "$(hostname)"
printf '      %-42s %s\n' kernel "$(uname -r)"
printf '      %-42s %s\n' "cpu model" "$(sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -n 1)"
printf '      %-42s %s\n' "online cpus" "$(read_file /sys/devices/system/cpu/online)"
virt="$(systemd-detect-virt 2>/dev/null || true)"
printf '      %-42s %s\n' virtualization "${virt:-none}"
printf '      %-42s %s\n' "kernel command line" "$(cat /proc/cmdline)"

echo "== clocksource"
clocksource="$(read_file /sys/devices/system/clocksource/clocksource0/current_clocksource)"
printf '      %-42s %s\n' current "$clocksource"
printf '      %-42s %s\n' available "$(read_file /sys/devices/system/clocksource/clocksource0/available_clocksource)"
case "$clocksource" in
    tsc | kvm-clock) ;;
    *) echo "  WARNING: clocksource $clocksource has no vDSO fast path; timing will be slow and noisy" ;;
esac

echo "== benchmark sysctls"
for setting in "${bench_sysctls[@]}"; do
    ensure_sysctl "$setting"
done
# The benchmark listeners (19xxx) sit below the default ephemeral range
# (32768-60999); run-m0.sh refuses ports inside the range, so it is only
# reported here.
report_sysctl net.ipv4.ip_local_port_range
report_sysctl net.ipv4.ip_local_reserved_ports

echo "== deployment tips (fq, bbr)"
if ((deploy_tips)); then
    if [[ " $(read_sysctl net.ipv4.tcp_available_congestion_control) " != *" bbr "* ]]; then
        as_root modprobe tcp_bbr
    fi
    for setting in "${deploy_sysctls[@]}"; do
        ensure_sysctl "$setting"
    done
else
    for setting in "${deploy_sysctls[@]}"; do
        printf '      %-42s %s (deployment tip: %s; not applied without --deploy-tips)\n' \
            "${setting%%=*}" "$(read_sysctl "${setting%%=*}")" "${setting#*=}"
    done
    report_sysctl net.ipv4.tcp_available_congestion_control
fi

echo "== cpu frequency"
shopt -s nullglob
governors=(/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor)
shopt -u nullglob
if ((${#governors[@]} == 0)); then
    printf '      %-42s not exposed on this host (virtual machine?), skipped\n' "scaling_governor"
else
    for file in "${governors[@]}"; do
        ensure_sysfs performance scaling_governor "$file"
    done
fi
# acpi-cpufreq and amd-pstate expose boost; intel_pstate inverts it as no_turbo.
if [[ -e /sys/devices/system/cpu/cpufreq/boost ]]; then
    ensure_sysfs 0 boost /sys/devices/system/cpu/cpufreq/boost
else
    ensure_sysfs 1 "boost / no_turbo" /sys/devices/system/cpu/intel_pstate/no_turbo
fi
if [[ -e /sys/devices/system/cpu/amd_pstate/status ]]; then
    printf '      %-42s %s\n' amd_pstate "$(read_file /sys/devices/system/cpu/amd_pstate/status)"
fi

echo "== memory"
printf '      %-42s %s\n' "transparent_hugepage/enabled" "$(read_file /sys/kernel/mm/transparent_hugepage/enabled)"
printf '      %-42s %s\n' "transparent_hugepage/defrag" "$(read_file /sys/kernel/mm/transparent_hugepage/defrag)"
report_sysctl vm.swappiness
printf '      %-42s %s\n' "swap in use (KiB)" "$(awk '/^SwapTotal/ {t=$2} /^SwapFree/ {f=$2} END {print t - f}' /proc/meminfo)"

echo "== scheduling and interrupts"
report_sysctl kernel.sched_autogroup_enabled
report_sysctl kernel.numa_balancing
printf '      %-42s %s\n' irqbalance "$(systemctl is-active irqbalance 2>/dev/null || true)"
report_sysctl net.core.netdev_max_backlog
# Column 2 of softnet_stat counts packets dropped because a CPU's backlog was
# full; loopback traffic passes through the same backlog.
softnet_drops=0
while read -r _ dropped _; do
    softnet_drops=$((softnet_drops + 16#$dropped))
done </proc/net/softnet_stat
printf '      %-42s %s\n' "softnet backlog drops (all cpus)" "$softnet_drops"
# Where the NIC queues interrupt, so a session can be audited for interrupts
# landing on the gateway's CPUs.
while read -r irq name; do
    printf '      %-42s cpus %s\n' "irq $irq ($name)" "$(read_file "/proc/irq/$irq/smp_affinity_list")"
done < <(awk '$1 ~ /^[0-9]+:$/ && /virtio|eth|ens|enp/ { sub(":", "", $1); print $1, $NF }' /proc/interrupts)

echo "== limits"
printf '      %-42s soft %s, hard %s (the bench tools raise soft to hard)\n' RLIMIT_NOFILE "$(ulimit -Sn)" "$(ulimit -Hn)"
report_sysctl fs.nr_open

if ((apply && persist)); then
    {
        echo "$persist_marker"
        for setting in "${bench_sysctls[@]}"; do
            echo "${setting%%=*} = ${setting#*=}"
        done
        if ((deploy_tips)); then
            for setting in "${deploy_sysctls[@]}"; do
                echo "${setting%%=*} = ${setting#*=}"
            done
        fi
    } | as_root tee "$persist_file" >/dev/null
    # The originals must outlive the reboot that re-applies the file.
    if [[ -f "$runtime_state" ]]; then
        while IFS=$'\t' read -r kind key value; do
            remember_in "$persist_state" "$kind" "$key" "$value"
        done <"$runtime_state"
    fi
    echo "== persisted sysctls to $persist_file"
fi

if ((apply)); then
    lifetime="runtime only, a reboot undoes them"
    ((persist)) && lifetime="also persisted to $persist_file"
    echo "== $changed setting(s) changed ($lifetime); originals in $runtime_state"
    if ((${#rollback[@]} > 0)); then
        echo "== rollback by hand (equivalent to --restore for this run's changes):"
        printf '      %s\n' "${rollback[@]}"
    fi
elif ((check)); then
    if ((unmet > 0)); then
        echo "== check: $unmet setting(s) marked [ ]; run with --apply"
        exit 3
    fi
    echo "== check: host prepared"
else
    echo "== report only; run with --apply to change the settings marked [ ]"
fi

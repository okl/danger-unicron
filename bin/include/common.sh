# common.sh: assorted bash functions

LOG_FACILITY=local0
LOG_TAG=$(basename -- "$0")
GNU_READLINK=$(readlink -f . >/dev/null 2>&1 && echo true || echo false)

# log(level, ...): log a message to syslog $level
log () {
    level=$1
    shift
    logger -p ${LOG_FACILITY}.$level -t "$LOG_TAG[$$]" -- $@
}

# die(...): print and log arguments to stderr and exit with status code 1
die () {
    echo "[!] $@" >&2
    log err $@
    exit 1
}

# warn(...): print a warning and log
warn () {
    echo "[~] $@"
    log warn $@
}

# info(...): print and log arguments to stdout
info () {
    echo "[*] $@"
    log info $@
}

# debug(...): print and log to debug
debug () {
    echo "[.] $@"
    log debug $@
}

# canonpath(file): follow syslinks and print canonicalized pathname
# NOTE: This should be called instead of GNU's readlink -f.
canonpath () {
    if $GNU_READLINK; then
        echo $(readlink -f "$1")
    else
        perl -e 'use Cwd qw(abs_path); print abs_path($ARGV[0]) . "\n"' "$1"
    fi
}

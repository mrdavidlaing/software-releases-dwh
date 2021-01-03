#!/usr/bin/env bash

# A best practices Bash script template with many useful functions. This file
# sources in the bulk of the functions from the source.sh file which it expects
# to be in the same directory. Only those functions which are likely to need
# modification are present in this file. This is a great combination if you're
# writing several scripts! By pulling in the common functions you'll minimise
# code duplication, as well as ease any potential updates to shared functions.

# Enable xtrace if the DEBUG environment variable is set
if [[ ${DEBUG-} =~ ^1|yes|true$ ]]; then
    set -o xtrace       # Trace the execution of the script (debug)
fi

# A better class of script...
set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline

# DESC: Usage help
# ARGS: None
# OUTS: None
function script_usage() {
    cat << EOF
Port forwards to current Dagit instance running on europe_belgium k8s cluster

Dependencies: gcloud, kubectl
Usage:
     -h|--help                  Displays this help
     -v|--verbose               Displays verbose output
    -nc|--no-colour             Disables colour output
    -cr|--cron                  Run silently unless we encounter an error
EOF
}

# DESC: Parameter parser
# ARGS: $@ (optional): Arguments provided to the script
# OUTS: Variables indicating command-line parameters and options
function parse_params() {
    local param
    while [[ $# -gt 0 ]]; do
        param="$1"
        shift
        case $param in
            -h | --help)
                script_usage
                exit 0
                ;;
            -v | --verbose)
                verbose=true
                ;;
            -nc | --no-colour)
                no_colour=true
                ;;
            -cr | --cron)
                cron=true
                ;;
            *)
                script_exit "Invalid parameter was provided: $param" 1
                ;;
        esac
    done
}

function target_europe_belgium_k8s_cluster(){
  pretty_print "====> Setting KUBECONFIG to target GCP:europe-belgium k8s cluster" "$ta_bold"
  export KUBECONFIG="$script_dir/../secrets/europe-belgium.kubeconfig.yaml"
  gcloud container clusters get-credentials europe-belgium --zone europe-west1-d --project mrdavidlaing
}

function port_forward_to_dagit() {
  pretty_print "====> Port forwarding to dagit" "$ta_bold"
  export DAGIT_POD_NAME=$(kubectl get pods --namespace dagster -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=dagster,component=dagit" -o jsonpath="{.items[0].metadata.name}")
  kubectl --namespace dagster port-forward $DAGIT_POD_NAME 8080:80
}

# DESC: Main control flow
# ARGS: $@ (optional): Arguments provided to the script
# OUTS: None
function main() {
    source "$(dirname "${BASH_SOURCE[0]}")/ralish_bash-script-template.sh"

    trap script_trap_err ERR
    trap script_trap_exit EXIT

    script_init "$@"
    parse_params "$@"
    cron_init
    colour_init

    check_binary gcloud "halt-if-not-exists"
    check_binary kubectl "halt-if-not-exists"

    target_europe_belgium_k8s_cluster
    port_forward_to_dagit
}

# Make it rain
main "$@"

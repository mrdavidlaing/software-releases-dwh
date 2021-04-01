#!/usr/bin/env bash

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
Builds & updates docker container being used to run Dagster Cloud Run deployment

Dependencies: gcloud, docker
Usage:
     -sb|--skip-build           Skips building & pushing a new version of the Dagster container
     -h|--help                  Displays this help
     -v|--verbose               Displays verbose output
     -nc|--no-colour            Disables colour output
     -cr|--cron                 Run silently unless we encounter an error
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
            -sb | --skip-build)
                skip_build=true
                ;;
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

function build_and_push_container() {
  cal_version=$(date +'%Y-%m-%d.%s')

  pretty_print "====> Building & pushing new image: eu.gcr.io/mrdavidlaing/software-releases-dwh-dagster-cloudrun:$cal_version" "$ta_bold"
  pushd $(realpath "$script_dir/../")
    PIPENV_VERBOSITY=-1 pipenv lock --requirements > tmp/requirements.txt
    docker build -f "dagster_instances/gcp-cloud-run-europe-west1/Dockerfile" . \
    --tag eu.gcr.io/mrdavidlaing/software-releases-dwh-dagster-cloudrun:$cal_version \
    --tag eu.gcr.io/mrdavidlaing/software-releases-dwh-dagster-cloudrun:latest
  popd
  docker push eu.gcr.io/mrdavidlaing/software-releases-dwh-dagster-cloudrun:$cal_version
  docker push eu.gcr.io/mrdavidlaing/software-releases-dwh-dagster-cloudrun:latest
}

function update_helm_deployment() {
  if [ -z "${skip_build-}" ]; then # If skip_build NOT set
    pretty_print "====> Updating helm deployment: dagster (new image tag=$cal_version)" "$ta_bold"
    helm upgrade --namespace dagster --install dagster dagster/dagster -f "$script_dir/../helm_values.yaml" \
      --set userDeployments.deployments[0].image.tag="$cal_version" \
      --wait
  else
     pretty_print "====> Updating helm deployment: dagster" "$ta_bold"
     helm upgrade --namespace dagster --install dagster dagster/dagster -f "$script_dir/../helm_values.yaml" \
     --wait
  fi
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
    check_binary docker "halt-if-not-exists"

    if [ -z "${skip_build-}" ]; then # If skip_build NOT set
      build_and_push_container
    fi
#    target_europe_belgium_k8s_cluster
#    update_helm_deployment

    pretty_print "====> Completed successfully" "$ta_bold"
    pretty_print "Use $script_dir/connect_to_dagit.sh to open connection to Dagit"
}

# Make it rain
main "$@"

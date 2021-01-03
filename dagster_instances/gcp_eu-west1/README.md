

## Identity and credentials
* Solids need access to a Google Service Account with write permissions for:
  * [gs://software_releases_datalake](https://console.cloud.google.com/storage/browser/software_releases_datalake) - need `Storage Admin`
  * [bigquery://mrdavidlaing:software_releases_dwh](https://console.cloud.google.com/bigquery?authuser=2&project=mrdavidlaing&supportedpurview=project&p=mrdavidlaing&d=software_releases_dwh&page=dataset) - need `BigQuery Data Editor` role
  * [bigquery](https://console.cloud.google.com/bigquery?authuser=2&project=mrdavidlaing&supportedpurview=project&p=mrdavidlaing&d=software_releases_datalake&page=dataset) - need `BigQuery Data Editor` role
  * A course grained way to achieve this is set the Node VM's default service account to one with the correct permissions
    (eg: `software-releases-pipeline@mrdavidlaing.iam.gserviceaccount.com`); which will then be inherited by any pods running on that node.
    Enable this by setting the Node Pool's [instance group's template](https://console.cloud.google.com/compute/instanceGroups/list?project=mrdavidlaing) 
    to use:
      * Service Account: `software-releases-pipeline@mrdavidlaing.iam.gserviceaccount.com`
      * Cloud API access scopes: `Allow full access to all Cloud APIs` 
    To enable the GKE nodes to send metrics & logginng data to Stackdriver; this account also needs:
      * `Monitoring Metric Writer`
      * `Logs Writer`
      * `Stackdriver Resource Metadata Writer`
   


* Additional secrets needed by solids can be passed via k8s secrets 
  (which will be exposed as env: vars in the containers executing the solids)
    ```
    kubectl --namespace dagster create secret generic software-releases-dwh-secrets --from-literal=GITHUB_TOKEN='6f61a5191XXXXXXXXXXXXXXXXX'
    ```
    * Pull these into job execution container via run config:
      `execution.celery-k8s-config.env-secrets=[software-releases-dwh-secrets]`
    
 
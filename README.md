# explore-dataflow

# dependency
* install apache-beam for gcp: `pipenv install 'apache-beam[gcp]'`

# notes
* `apache-beam` only works for python version 3.8.9 and below
* using `'apache-beam[gcp]'`, will automatically create dataflow job when run the script
* can create multiple job in dataflow with the same `job_name`
* apache-beam runners: https://beam.apache.org/documentation/#available-runners
* `WriteToBigquery` table shema definition: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema
* `writeDisposition` & `createDisposition` options: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job
* `createDispposition`, `writeDisposition`, `schema`: https://beam.apache.org/documentation/io/built-in/google-bigquery/#writing-to-bigquery
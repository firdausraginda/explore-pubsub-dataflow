# explore-dataflow

# dependency
* install apache-beam for gcp: `pipenv install 'apache-beam[gcp]'`

# notes
* `apache-beam` only works for python version 3.8.9 and below
* using `'apache-beam[gcp]'`, will automatically create dataflow job when run the script
* can create multiple job in dataflow with the same `job_name`

* apache-beam runners: https://beam.apache.org/documentation/#available-runners
* `createDispposition`, `writeDisposition`, `schema`, & **bigquery data type** : https://beam.apache.org/documentation/io/built-in/google-bigquery/#writing-to-bigquery
* using `FlatMap` in apache-beam: https://beam.apache.org/documentation/transforms/python/elementwise/flatmap/
* using `ParDo` in apache-beam: https://beam.apache.org/documentation/transforms/python/elementwise/pardo/
* using `CombinePerKey` to count words: https://beam.apache.org/documentation/transforms/python/aggregation/combineperkey/

* using `readfrompubsub`: https://beam.apache.org/releases/pydoc/2.8.0/apache_beam.io.gcp.pubsub.html?highlight=readfrompubsub
* when pipeline read from pubsub, it has to be a **streaming** pipeline
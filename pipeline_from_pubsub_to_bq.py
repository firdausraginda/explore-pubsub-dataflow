import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from setup_gcp import setup_creds
import re
import json


# setup credential gcp
setup_creds()


beam_options = PipelineOptions(
    streaming=True
)


with beam.Pipeline(options=beam_options) as p:
    (
        p
        | beam.io.ReadFromPubSub(
                subscription='projects/another-dummy-project-337513/subscriptions/dummy-default-topic-sub'
            )
        | beam.Map(print)
    )

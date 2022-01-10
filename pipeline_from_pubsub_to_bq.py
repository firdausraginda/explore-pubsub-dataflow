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
        | 'read from pubsub topic' >> beam.io.ReadFromPubSub(
                topic='projects/another-dummy-project-337513/topics/dummy-default-topic',
            )
        | 'convert bytes to string' >> beam.Map(lambda msg: msg.decode("utf-8") )
        | beam.Map(print)
    )

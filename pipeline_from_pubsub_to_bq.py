import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from setup_gcp import setup_creds


def convert_to_table_format(data):

    tabular_dict = {}
    tabular_dict['message'] = data
    
    return tabular_dict


# setup credential gcp
setup_creds()


# beam_options = PipelineOptions(
#     runner='DataflowRunner',
#     project='another-dummy-project-337513',
#     job_name='dummy-job-2',
#     temp_location='gs://dummy-dataflow-temp/temp',
#     region='us-central1'
# )

beam_options = PipelineOptions(
    streaming=True
)


with beam.Pipeline(options=beam_options) as p:
    read_from_pubsub = (
        p
        | 'read from pubsub topic' >> beam.io.ReadFromPubSub(
                topic='projects/another-dummy-project-337513/topics/dummy-default-topic',
            )
        | 'convert bytes to string' >> beam.Map(lambda msg: msg.decode("utf-8") )
        # | beam.Map(print)
    )

    (
        read_from_pubsub
        | 'convert to tabular format' >> beam.Map(convert_to_table_format)
        | 'write to bq' >> beam.io.WriteToBigQuery(
            "another-dummy-project-337513:dummy_dataset.words_count",
            schema='message:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location='gs://dummy-dataflow-temp/temp'
            )
    )

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

input_file = 'gs://dummy-bucket-files/lorem.txt'
output_path = 'gs://dummy-bucket-files/result.txt'

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='another-dummy-project-337513',
    job_name='dummy-job-1',
    temp_location='gs://dummy-dataflow-temp/temp',
    region='us'
)

with beam.Pipeline(options=beam_options) as p:
    pass
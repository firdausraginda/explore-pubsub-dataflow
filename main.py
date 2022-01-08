import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from setup_gcp import setup_creds
import re
import json


input_file = 'gs://dummy-bucket-files/dummy_text_1.txt'
output_path = 'gs://dummy-bucket-files/result.txt'

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='another-dummy-project-337513',
    job_name='dummy-job-1',
    temp_location='gs://dummy-dataflow-temp/temp',
    region='us-central1'
)

# setup credential gcp
setup_creds()


def print_data(data):
    print(data)

def to_json(data):
    json_ob = {}

    for item in data:
        if item in json_ob:
            json_ob[item] += 1
        else:
            json_ob[item] = 1

    return json_ob


# with beam.Pipeline(options=beam_options) as p:
with beam.Pipeline() as p:
    pre_processing = (
        p
        | 'read file' >> beam.io.ReadFromText(input_file)
        | beam.Map(lambda words: words.lower())
        | beam.Map(lambda words: re.sub(r'[^\w]', ' ', words))
        | 'remove white spaces before and after string' >> beam.Map(lambda words: words.strip())
        | 'remove multiple white spaces between string' >> beam.Map(lambda words: re.sub(r' +', ' ', words))
        | 'split to list' >> beam.Map(lambda words: words.split(' '))
        # | 'print result' >> beam.Map(print_data)
    )

    convert_to_json = (
        pre_processing
        | 'convert to json' >> beam.Map(to_json)
        | 'serialize' >> beam.Map(lambda data: json.dumps(data))
        # | 'print result json' >> beam.Map(print_data)
    )

    (
        convert_to_json
        | beam.io.WriteToText('result', 
            file_name_suffix='.json'
            )
    )
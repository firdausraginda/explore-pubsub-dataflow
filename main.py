import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from setup_gcp import setup_creds
import re
import json


input_file = 'gs://dummy-bucket-files/dummy_text_1.txt'
output_path = 'gs://dummy-bucket-files/word_count_result_1'

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='another-dummy-project-337513',
    job_name='dummy-job-2',
    temp_location='gs://dummy-dataflow-temp/temp',
    region='us-central1'
)

# setup credential gcp
setup_creds()


def print_data(data):
    print(data)

def count_words_in_json(data):
    json_ob = {}

    for item in data:
        if item in json_ob:
            json_ob[item] += 1
        else:
            json_ob[item] = 1

    return json_ob


def convert_to_table_format(data):
    tabular_dict_list = []

    for key, value in data.items():
        tabular_dict = {}

        tabular_dict['word'] = key
        tabular_dict['count'] = value

        tabular_dict_list.append(tabular_dict)
    
    return tabular_dict_list


word_count_schema = """
    word:STRING,
    count:INTEGER
"""

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

    count_in_json = (
        pre_processing
        | 'convert to json' >> beam.Map(count_words_in_json)
        # | 'print result json' >> beam.Map(print_data)
    )

    # (
    #     count_in_json
    #     | 'serialize' >> beam.Map(lambda data: json.dumps(data))
    #     | 'write to local' >> beam.io.WriteToText('result', 
    #         file_name_suffix='.json'
    #         )
    # )

    # (
    #     count_in_json
    #     | 'serialize' >> beam.Map(lambda data: json.dumps(data))
    #     | 'write to gcs' >> beam.io.WriteToText(output_path, file_name_suffix='.json')
    # )

    (
        count_in_json
        | 'convert to dictionary tabular format' >> beam.Map(convert_to_table_format)
        # | 'print result converted type' >> beam.Map(print_data)
        | 'write to bq' >> beam.io.WriteToBigQuery(
            "another-dummy-project-337513:dummy_dataset.words_count",
            schema=word_count_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location='gs://dummy-dataflow-temp/temp'
        )
    )
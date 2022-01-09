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

def convert_to_table_format(data):

    tabular_dict = {}
    tabular_dict['word'] = data[0]
    tabular_dict['count'] = data[1]
    
    return tabular_dict


word_count_schema = {
    'fields': [{
        'name': 'word', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'count', 'type': 'INTEGER', 'mode': 'NULLABLE'
    }]
}


# with beam.Pipeline(options=beam_options) as p:
with beam.Pipeline() as p:

    pre_processing = (
        p
        | 'read file' >> beam.io.ReadFromText(input_file)
        | 'lowercase' >> beam.Map(lambda words: words.lower())
        | 'remove symbols in string' >> beam.Map(lambda words: re.sub(r'[^\w]', ' ', words))
        | 'remove white spaces before and after string' >> beam.Map(lambda words: words.strip())
        | 'remove multiple white spaces between string' >> beam.Map(lambda words: re.sub(r' +', ' ', words))
        | 'split to list' >> beam.Map(lambda words: words.split(' '))
        | 'flatten PCollection lists of strings to PCollection of strings' >> beam.FlatMap(lambda word: word)
        # | 'print result' >> beam.Map(print)
    )

    count_as_json = (
        pre_processing
        | 'create tuple of words with num of appearances' >> beam.Map(lambda word: (word, 1))
        | 'count words' >> beam.CombinePerKey(sum)
        | 'convert to table format' >> beam.Map(convert_to_table_format)
        # | beam.Map(print)
    )

    serialize = (
        count_as_json
        | 'serialize' >> beam.Map(lambda data: json.dumps(data))
    )

    # write to local as json file
    (
        serialize
        | 'write to local' >> beam.io.WriteToText('result', file_name_suffix='.json')
    )

    # write to gcs as json file
    (
        serialize
        | 'write to gcs' >> beam.io.WriteToText(output_path, file_name_suffix='.json')
    )

    # write to bq table by loop the data using flatmap & lambda function
    (
        count_as_json
        | 'write to bq' >> beam.io.WriteToBigQuery(
            "another-dummy-project-337513:dummy_dataset.words_count",
            schema=word_count_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            custom_gcs_temp_location='gs://dummy-dataflow-temp/temp'
            )
    )
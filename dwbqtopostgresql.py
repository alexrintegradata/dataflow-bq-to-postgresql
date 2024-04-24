import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
# from google.cloud import bigquery
import requests
import argparse
import logging


def checkpgtobq(element, table_name):
    res = requests.post(
        'https://southamerica-west1-nicanor-dev.cloudfunctions.net/integradata_replica', json={"table_name": table_name})
    return 1


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    # Configurar las opciones del pipeline
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    # Configurar las opciones del pipeline
    pipeline = beam.Pipeline(options=pipeline_options)

    check_consolidated_table = (
        pipeline
        | "Inicializa" >> beam.Create([None])
        | "Actualiza Prem_dotacion" >> beam.Map(checkpgtobq, 'prem_dotacion')
    )

    pipeline.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# pip install -r requirements.txt
# python dwbqtopostgresql.py  --runner DataflowRunner --temp_location gs://bbss_cierre/tmp --project nicanor-dev --region us-central1 --job_name dwbqtopostgresql --template_location gs://bbss_cierre/template/dwbqtopostgresql

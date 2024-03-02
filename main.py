import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime
class FilterTransactions(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | beam.Filter(filter_transactions)
        )
    
def filter_transactions(transaction):
    # Split the transaction by comma
    transaction_fields = transaction.split(',')
    
    # Extract the transaction timestamp (assuming it's the second column) and convert to datetime object
    transaction_timestamp = datetime.datetime.strptime(transaction_fields[0], '%Y-%m-%d %H:%M:%S %Z')

    # Check if the transaction timestamp is after 2010-01-01
    return transaction_timestamp.year >= 2010 and float(transaction_fields[3]) > 20

def extract_date_amount(transaction):
    transaction_fields = transaction.split(',')
    transaction_timestamp = datetime.datetime.strptime(transaction_fields[0], '%Y-%m-%d %H:%M:%S %Z')
    return (transaction_timestamp.strftime('%Y-%m-%d'), float(transaction_fields[3]))

def sum_by_date(transactions):
    date, amounts = transactions
    return date, sum(amounts)

def run():
    options = PipelineOptions(runner='DirectRunner')
    with beam.Pipeline(options=options) as pipe:
        transactions = (
            pipe
            | 'ReadTransactions' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
            | 'FilterTransactions' >> beam.Filter(filter_transactions)
            | 'ExtractDateAmount' >> beam.Map(extract_date_amount)
            | 'GroupByDate' >> beam.GroupByKey()
            | 'SumByDate' >> beam.Map(sum_by_date)
            | 'WriteOutput' >> beam.io.WriteToText('output/transactions_filtered', file_name_suffix='.csv')
        )

if __name__ == '__main__':
    run()

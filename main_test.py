import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import main

class TestCompositeTransform(unittest.TestCase):
    def test_composite_transform(self):
        with TestPipeline() as pipe:
            # Generate some test input data
            input_data = [
                '2011-01-01, 25.0',
                '2012-01-01, 30.0',
                '2009-01-01, 15.0'
            ]

            # Apply the composite transform to the test input data
            output = (
                pipe
                | beam.Create(input_data)
                | main.FilterTransactions()
                | 'ExtractDateAmount' >> beam.ParDo(main.extract_date_amount())
                | 'GroupByDate' >> beam.GroupByKey()
                | 'SumByDate' >> beam.ParDo(main.SumByDate())
            )

            # Define expected output
            expected_output = [
                ('2017-03-18', 2102.22)
                ('2017-08-31', 13700000023.08)
                ('2018-02-27', 129.12)
            ]

            # Check that the output matches the expected output
            assert_that(output, equal_to(expected_output))

if __name__ == '__main__':
    unittest.main()
# Apache Beam Pipeline Example

This repository contains an example Apache Beam pipeline implemented in Python. The pipeline processes transaction data, filters transactions based on criteria, and performs aggregations by date.

## Prerequisites

To run the Apache Beam pipeline, you need to have Python installed on your system. Additionally, ensure you have the Apache Beam library installed:

```bash
pip install apache-beam
```

## Usage

1. Clone this repository to your local machine:

```bash
git clone git@github.com:faisalm1997/apache_beam_pipeline.git
cd apache_beam_pipeline
```

2. Execute the Apache Beam pipeline by running the `main.py` script:

```bash
python3 main.py
```

## Pipeline Overview

The Apache Beam pipeline consists of several components:

### FilterTransactions Transform

The `FilterTransactions` transform filters transactions based on the transaction timestamp and amount. It is implemented as a composite transform that encapsulates the filtering logic.

### ExtractDateAmount Function

The `extract_date_amount` function extracts the date and amount from each transaction. It is used as a ParDo transform to process each element in the pipeline.

### SumByDate Function

The `sum_by_date` function calculates the total amount for each date. It is applied as a ParDo transform after grouping transactions by date.

## Testing

The `test_main.py` script contains unit tests for the Apache Beam pipeline. It uses the Apache Beam testing framework to define test cases and verify the correctness of the pipeline transformations.

To run the tests, execute the following command:

```bash
python3 test_main.py
```

## License

This project is licensed under the [MIT License](LICENSE).

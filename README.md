# geo2bq

Load a geography file to BigQuery

## Installation

```sh
# Create and activate a virtual environment (recommended)
virtualenv venv
source venv/bin/activate

# Install dependencies
make install
```

## Usage

### Command Line

```sh
$ python geo2bq.py --help

usage: geo2bq.py [-h] src_path dest_table_path

Load a geography file to BigQuery

positional arguments:
  src_path         path to a geo file in a supported format
  dest_table_path  destination project.dataset.table in BigQuery

optional arguments:
  -h, --help       show this help message and exit

```

#### Example

```sh
python geo2bq.py input-file.shp project.dataset.destination_table
```

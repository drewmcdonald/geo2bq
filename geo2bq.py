#! /usr/bin/env python

"""Load a geography file to BigQuery"""

import logging
import os.path
from random import choices
from tempfile import TemporaryDirectory

import geopandas as gpd
import google.cloud.bigquery as bq

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
log = logging.getLogger(__file__[:-3])

TARGET_CRS = "epsg:4326"


def create_table_from_csv(
    client: bq.Client, local_csv_path: str, table_ref: bq.TableReference
):
    log.info(f"Loading temporary csv to `{table_ref}`")
    with open(local_csv_path, "rb") as f:
        load_job = client.load_table_from_file(
            file_obj=f,
            destination=table_ref,
            job_config=bq.LoadJobConfig(
                autodetect=True,
                skip_leading_rows=1,
                write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
                source_format=bq.SourceFormat.CSV,
            ),
        )
        load_job.result()


def create_final_table(
    client: bq.Client, from_ref: bq.TableReference, to_ref: bq.TableReference
):
    log.info(f"Creating `{to_ref}` from `{from_ref}`")
    query = f"""
    DROP TABLE IF EXISTS {to_ref};  -- full drop in case of partition config mismatch
    CREATE TABLE {to_ref} CLUSTER BY geometry AS
    SELECT * EXCEPT(geometry), ST_GEOGFROMTEXT(geometry) as geometry FROM {from_ref}
    """
    query_job = client.query(query)
    query_job.result()


def load_gdf(src_path: str) -> gpd.GeoDataFrame:
    """Load a local gis file to a geodataframe; reproject it if needed"""
    log.info(f"Reading {src_path} as a geopandas GeoDataFrame")
    gdf = gpd.read_file(src_path)

    log.info(f"Current CRS is {gdf.crs}.")
    if gdf.crs == TARGET_CRS:
        return gdf

    log.info(f"Reprojecting to {TARGET_CRS}.")
    return gdf.to_crs(TARGET_CRS)


def dump_gdf_to_csv(gdf: gpd.GeoDataFrame, dir: str, filename: str = "data.csv") -> str:
    path = os.path.join(dir, filename)
    log.info(f"Dumping temporary csv to {path}")
    gdf.to_csv(path, index=False)
    return path


def cleanup(client: bq.Client, tmp_ref: bq.TableReference):
    log.info(f"Deleting intermediate table `{tmp_ref}`")
    client.delete_table(tmp_ref, not_found_ok=True)


def _tmp_table_ref(dest_ref: bq.TableReference) -> bq.TableReference:
    """Generate a randomly named table reference in the same dataset as the destination"""
    tmp_table_id = f"_tmp_{''.join(choices('abcdefghijklmnopqrstuvwxyz', k=12))}"
    return bq.TableReference.from_string(
        f"{dest_ref.project}.{dest_ref.dataset_id}.{tmp_table_id}"
    )


def geo2bq(client: bq.Client, src_path: str, dest_table_path: str):

    dest_ref = bq.TableReference.from_string(dest_table_path)
    tmp_ref = _tmp_table_ref(dest_ref)

    gdf = load_gdf(src_path)
    with TemporaryDirectory() as tmpdir:
        local_csv = dump_gdf_to_csv(gdf, tmpdir)
        try:
            create_table_from_csv(client, local_csv, tmp_ref)
            create_final_table(client, tmp_ref, dest_ref)
        finally:
            cleanup(client, tmp_ref)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("src_path", help="path to a geo file in a supported format")
    parser.add_argument(
        "dest_table_path", help="destination project.dataset.table in BigQuery"
    )

    args = parser.parse_args()
    geo2bq(
        client=bq.Client(),
        src_path=args.src_path,
        dest_table_path=args.dest_table_path,
    )

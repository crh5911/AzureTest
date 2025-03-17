import logging
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        data = req_body  # Assuming the KQL query results are passed as the request body

        # Convert JSON to Pandas DataFrame
        df = pd.DataFrame(data)

        # Convert Pandas DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write PyArrow Table to Parquet in memory
        parquet_buffer = io.BytesIO()
        pq.write_table(table, parquet_buffer)

        # Return Parquet data as bytes
        parquet_bytes = parquet_buffer.getvalue()

        return func.HttpResponse(
             parquet_bytes,
             mimetype="application/octet-stream"  # Or "application/x-parquet"
        )

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(
             f"Error: {e}",
             status_code=500
        )

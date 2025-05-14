from ibis.common.exceptions import TableNotFound
from google.api_core.exceptions import NotFound
import google.cloud.bigquery as bq_api
import ibis.expr.operations as ibis_ops
import sqlglot as sg
from ibis.backends.bigquery import Backend as BigQueryBackend
from ibis.backends.bigquery.datatypes import BigQuerySchema


def table_exists(
    backend: BigQueryBackend,
    table_name: str,
    database: tuple[str, str] | str | None = None,
) -> bool:
    try:
        _ = backend.table(table_name, database=database)
        return True
    except TableNotFound as e:
        return False
    except NotFound as e:
        if "Not found: Table" in e.message:
            return False
        else:
            raise


# FIX: temp fix until https://github.com/ibis-project/ibis/issues/9215 is closed
# used solution from https://github.com/ibis-project/ibis/issues/9216
def _register_in_memory_table(
    self: BigQueryBackend, op: ibis_ops.InMemoryTable
) -> None:
    raw_name = op.name

    assert self._session_dataset is not None
    project = self._session_dataset.project
    dataset = self._session_dataset.dataset_id

    if not table_exists(self, raw_name, database=(project, dataset)):
        table_id = sg.table(
            raw_name,
            db=dataset,
            catalog=project,
            quoted=False,
        ).sql(dialect=self.name)

        bq_schema = BigQuerySchema.from_ibis(op.schema)
        load_job = self.client.load_table_from_dataframe(
            op.data.to_frame(),
            table_id,
            job_config=bq_api.LoadJobConfig(
                # fail if the table already exists and contains data
                write_disposition=bq_api.WriteDisposition.WRITE_EMPTY,
                schema=bq_schema,
            ),
        )
        load_job.result()

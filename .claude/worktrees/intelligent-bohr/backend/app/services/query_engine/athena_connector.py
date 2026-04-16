"""
Athena Connector
=================

Handles the actual execution of queries against AWS Athena.
Implements:
- Query submission and polling
- Result fetching and pagination
- Query cost estimation
- Connection pooling via semaphore (max concurrent queries)
- Retry logic for transient failures
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass
class AthenaQueryResult:
    """Result of an Athena query execution."""
    query_execution_id: str
    status: str  # QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED
    rows: list[dict] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)
    total_rows: int = 0
    data_scanned_bytes: int = 0
    execution_time_ms: int = 0
    sql: str = ""
    error_message: str | None = None


class AthenaConnector:
    """
    Async wrapper around boto3 Athena client.

    Usage:
        connector = AthenaConnector(
            region="ap-south-1",
            max_concurrent=5,
        )
        result = await connector.execute(
            sql="SELECT ...",
            database="spencers_gold",
            workgroup="primary",
        )
    """

    def __init__(
        self,
        region: str = "ap-south-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        max_concurrent: int = 5,
        poll_interval_sec: float = 1.0,
        max_poll_attempts: int = 300,  # 5 minutes max
    ):
        self.region = region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.max_concurrent = max_concurrent
        self.poll_interval_sec = poll_interval_sec
        self.max_poll_attempts = max_poll_attempts
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._client = None

    def _get_client(self):
        """Lazy-initialize the boto3 Athena client."""
        if self._client is None:
            try:
                import boto3

                kwargs: dict[str, Any] = {"region_name": self.region}
                if self.aws_access_key_id:
                    kwargs["aws_access_key_id"] = self.aws_access_key_id
                    kwargs["aws_secret_access_key"] = self.aws_secret_access_key
                self._client = boto3.client("athena", **kwargs)
            except ImportError:
                logger.warning("boto3 not installed — Athena queries will be compiled but not executed")
                return None
            except Exception as e:
                logger.error(f"Failed to initialize Athena client: {e}")
                return None
        return self._client

    async def execute(
        self,
        sql: str,
        database: str,
        workgroup: str = "primary",
        output_location: str | None = None,
        max_rows: int = 10000,
    ) -> AthenaQueryResult:
        """
        Execute a SQL query on Athena and return results.

        Steps:
        1. Submit query via StartQueryExecution
        2. Poll GetQueryExecution until terminal state
        3. Fetch results via GetQueryResults (paginated)
        """
        async with self._semaphore:
            return await self._execute_impl(sql, database, workgroup, output_location, max_rows)

    async def _execute_impl(
        self,
        sql: str,
        database: str,
        workgroup: str,
        output_location: str | None,
        max_rows: int,
    ) -> AthenaQueryResult:
        client = self._get_client()
        if client is None:
            return AthenaQueryResult(
                query_execution_id="dry-run",
                status="DRY_RUN",
                sql=sql,
                error_message="Athena client not available — query compiled but not executed",
            )

        # 1. Start query execution
        start_params: dict[str, Any] = {
            "QueryString": sql,
            "QueryExecutionContext": {"Database": database},
            "WorkGroup": workgroup,
        }
        if output_location:
            start_params["ResultConfiguration"] = {"OutputLocation": output_location}

        try:
            response = await asyncio.to_thread(
                client.start_query_execution, **start_params
            )
            query_execution_id = response["QueryExecutionId"]
            logger.info(f"Athena query submitted: {query_execution_id} on {database}")
        except Exception as e:
            logger.error(f"Failed to submit Athena query: {e}")
            return AthenaQueryResult(
                query_execution_id="error",
                status="FAILED",
                sql=sql,
                error_message=str(e),
            )

        # 2. Poll for completion
        status = "RUNNING"
        data_scanned = 0
        execution_time = 0

        for attempt in range(self.max_poll_attempts):
            await asyncio.sleep(self.poll_interval_sec)

            try:
                exec_response = await asyncio.to_thread(
                    client.get_query_execution,
                    QueryExecutionId=query_execution_id,
                )
                execution = exec_response["QueryExecution"]
                status = execution["Status"]["State"]

                if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    stats = execution.get("Statistics", {})
                    data_scanned = stats.get("DataScannedInBytes", 0)
                    execution_time = stats.get("EngineExecutionTimeInMillis", 0)
                    break

            except Exception as e:
                logger.warning(f"Poll attempt {attempt} failed: {e}")
                continue

        if status == "FAILED":
            error = exec_response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            return AthenaQueryResult(
                query_execution_id=query_execution_id,
                status="FAILED",
                sql=sql,
                data_scanned_bytes=data_scanned,
                execution_time_ms=execution_time,
                error_message=error,
            )

        if status != "SUCCEEDED":
            return AthenaQueryResult(
                query_execution_id=query_execution_id,
                status=status,
                sql=sql,
                error_message=f"Query did not complete within {self.max_poll_attempts * self.poll_interval_sec}s",
            )

        # 3. Fetch results
        rows = []
        column_names = []
        next_token = None

        while len(rows) < max_rows:
            fetch_params: dict[str, Any] = {
                "QueryExecutionId": query_execution_id,
                "MaxResults": min(1000, max_rows - len(rows)),
            }
            if next_token:
                fetch_params["NextToken"] = next_token

            try:
                result_response = await asyncio.to_thread(
                    client.get_query_results, **fetch_params
                )
            except Exception as e:
                logger.error(f"Failed to fetch results: {e}")
                break

            result_set = result_response["ResultSet"]

            # Extract column names from first page
            if not column_names:
                column_names = [
                    col["Name"]
                    for col in result_set["ResultSetMetadata"]["ColumnInfo"]
                ]

            # Parse rows (skip header row on first page)
            for i, row in enumerate(result_set["Rows"]):
                if not rows and i == 0 and not next_token:
                    continue  # Skip header row
                row_data = {}
                for j, datum in enumerate(row["Data"]):
                    col_name = column_names[j] if j < len(column_names) else f"col_{j}"
                    row_data[col_name] = datum.get("VarCharValue")
                rows.append(row_data)

            next_token = result_response.get("NextToken")
            if not next_token:
                break

        return AthenaQueryResult(
            query_execution_id=query_execution_id,
            status="SUCCEEDED",
            rows=rows,
            column_names=column_names,
            total_rows=len(rows),
            data_scanned_bytes=data_scanned,
            execution_time_ms=execution_time,
            sql=sql,
        )

    async def estimate_cost(self, data_scanned_bytes: int) -> float:
        """Estimate query cost based on data scanned. Athena charges $5/TB."""
        tb_scanned = data_scanned_bytes / (1024 ** 4)
        return round(tb_scanned * 5.0, 6)

    async def get_table_metadata(self, database: str, table_name: str) -> dict:
        """Fetch table metadata (columns, partitions, row count estimate)."""
        client = self._get_client()
        if not client:
            return {"error": "Client not available"}

        try:
            response = await asyncio.to_thread(
                client.get_table_metadata,
                CatalogName="AwsDataCatalog",
                DatabaseName=database,
                TableName=table_name,
            )
            metadata = response["TableMetadata"]
            return {
                "name": metadata["Name"],
                "columns": [
                    {"name": col["Name"], "type": col["Type"], "comment": col.get("Comment", "")}
                    for col in metadata.get("Columns", [])
                ],
                "partition_keys": [
                    {"name": col["Name"], "type": col["Type"]}
                    for col in metadata.get("PartitionKeys", [])
                ],
                "parameters": metadata.get("Parameters", {}),
            }
        except Exception as e:
            return {"error": str(e)}

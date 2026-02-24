import logging
from dataclasses import dataclass
from enum import Enum

from iomete_sdk.api_utils import APIUtils


class Flow(str, Enum):
    LEGACY = "LEGACY"
    PRIORITY = "PRIORITY"


class Priority(str, Enum):
    NORMAL = "NORMAL"
    HIGH = "HIGH"


@dataclass
class SparkJobApiClient:
    logger = logging.getLogger('SparkJobApiClient')

    host: str
    api_key: str
    domain: str
    verify: bool = True

    spark_job_endpoint: str = None
    api_utils: APIUtils = None

    def __post_init__(self):
        self.api_utils = APIUtils(api_key=self.api_key, verify=self.verify)

        self.logger.debug(f"Host: {self.host}")
        self.spark_job_endpoint = f"{self.host}/api/v2/domains/{self.domain}/sdk/spark/jobs"

    def _validate_job_payload(self, payload: dict):
        """Validate required fields and enum values for v2 job payloads."""

        if "flow" in payload:
            if payload["flow"] not in [e.value for e in Flow]:
                raise ValueError(f"flow must be one of: {[e.value for e in Flow]}")

        if "priority" in payload:
            if payload["priority"] not in [e.value for e in Priority]:
                raise ValueError(f"priority must be one of: {[e.value for e in Priority]}")

    def create_job(self, payload: dict):
        self._validate_job_payload(payload)

        if "bundleId" not in payload:
            raise ValueError("bundleId is required in job payload")

        return self.api_utils.call(method="POST", url=self.spark_job_endpoint, payload=payload)

    def update_job(self, job_id: str, payload: dict):
        self._validate_job_payload(payload)
        return self.api_utils.call(method="PUT", url=f"{self.spark_job_endpoint}/{job_id}", payload=payload)

    def get_jobs(self):
        response = self.api_utils.call(method="GET", url=self.spark_job_endpoint)
        return response.get("items", []) if isinstance(response, dict) else response

    def get_job_by_id(self, job_id: str):
        return self.api_utils.call(method="GET", url=f"{self.spark_job_endpoint}/{job_id}")

    def get_job_by_name(self, job_name: str):
        return self.api_utils.call(method="GET", url=f"{self.spark_job_endpoint}/name/{job_name}")

    def delete_job_by_id(self, job_id: str):
        return self.api_utils.call(method="DELETE", url=f"{self.spark_job_endpoint}/{job_id}")

    def get_job_runs(self, job_id: str):
        return self.api_utils.call(method="GET", url=f"{self.spark_job_endpoint}/{job_id}/runs")

    def submit_job_run(self, job_id: str, payload: dict):
        return self.api_utils.call(method="POST", url=f"{self.spark_job_endpoint}/{job_id}/runs", payload=payload)

    def cancel_job_run(self, job_id: str, run_id: str):
        return self.api_utils.call(method="DELETE", url=f"{self.spark_job_endpoint}/{job_id}/runs/{run_id}")

    def get_job_run_by_id(self, job_id: str, run_id: str):
        return self.api_utils.call(method="GET", url=f"{self.spark_job_endpoint}/{job_id}/runs/{run_id}")

    def get_job_run_logs(self, job_id: str, run_id: str, time_range: str = "5m"):
        return self.api_utils.call(method="GET",
                                   url=f"{self.spark_job_endpoint}/{job_id}/runs/{run_id}/logs?range={time_range}")

    def get_job_run_metrics(self, job_id: str, run_id: str):
        return self.api_utils.call(method="GET", url=f"{self.spark_job_endpoint}/{job_id}/runs/{run_id}/metrics")

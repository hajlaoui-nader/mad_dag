from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import subprocess


class CSVProcessorOperator(BaseOperator):
    @apply_defaults
    def __init__(self, s3_bucket, processing_script, *args, **kwargs):
        super(CSVProcessorOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.processing_script = processing_script

    def execute(self, context):
        dagrun = context["dag_run"].conf
        month = dagrun.get("month")

        year = dagrun.get("year")
        s3_key = f"{year}/{month}/events.csv"
        self.log.info(f"Processing CSV file from S3: {s3_key}")

        try:
            result = subprocess.run(
                [
                    "python",
                    self.processing_script,
                    self.s3_bucket,
                    s3_key,
                    month,
                    year,
                ],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.log.info(f"csv processor output: {result.stdout}")
            # Push the result (or file path) to XCom
            return_value = result.stdout.strip()
            context["ti"].xcom_push(key="processed_file_path", value=return_value)

        except subprocess.CalledProcessError as e:
            self.log.error("Error: %s", e.stderr)
            raise

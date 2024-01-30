import pandas as pd
import sys
import boto3
from botocore.config import Config
from botocore import UNSIGNED
import os
import uuid


def assure_true_duplicates(df):
    # Find duplicates based on 'id'
    duplicates = df[df.duplicated("id", keep=False)]

    # Iterate over each duplicate
    for idx, group in duplicates.groupby("id"):
        # Sort the group to compare rows pairwise
        sorted_group = group.sort_values(by=list(group.columns))

        # Compare each row with the next one in the sorted group
        for i in range(len(sorted_group) - 1):
            row1 = sorted_group.iloc[i]
            row2 = sorted_group.iloc[i + 1]

            # If any field is different, assign a new id to the second row
            if not row1.equals(row2):
                new_id = str(uuid.uuid4())  # Generate a unique id
                df.at[
                    row2.name, "id"
                ] = new_id  # Update the id in the original dataframe

    return df


def extract_company_from_email(email):
    """
    Extracts the company name from an email address.

    Args:
    email (str): The email address.

    Returns:
    str: The extracted company name.
    """
    try:
        # Split the email address at '@' and take the second part,
        # then split again at '.' and take the first part
        return email.split("@")[1].split(".")[0]
    except IndexError:
        # Handle cases where the email format is incorrect
        return "NA"


def process_csv(s3_bucket, s3_key, month, year):
    # the signature version is UNSIGNED to access public S3 buckets
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)

    month_str = str(month).zfill(2)
    year_str = str(year)

    # Read CSV file as dataframe
    df = pd.read_csv(obj["Body"])
    # filter df by date : need to be in month and year
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df[df["timestamp"].dt.strftime("%m") == month_str]
    df = df[df["timestamp"].dt.strftime("%Y") == year_str]
    # Add a new column for company extracted from email
    df["company"] = df["email"].apply(extract_company_from_email)
    # assure true duplicates
    df = assure_true_duplicates(
        df
    )  # IRL we would use a hash function to detect duplicates
    # in this example the file is saved in airflow temp folder (Not PROD ready)
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    output_dir = os.path.join(airflow_home, "temp", year, month)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "processed_data.csv")
    df.to_csv(output_file, index=False)
    # s3_client.upload_file("data.csv", s3_bucket, f"{year}/{month}/processed_data.csv") # IRL we would upload the file to S3
    print(output_file)


if __name__ == "__main__":
    s3_bucket = sys.argv[1]
    s3_key = sys.argv[2]
    month = sys.argv[3]
    year = sys.argv[4]
    process_csv(s3_bucket, s3_key, month, year)

import os
from datetime import datetime
from io import StringIO

import boto3
import pandas as pd


"""
AWS Lambda function that processes ATP tennis match data,
generates a Top 50 player ranking, stores the result in Amazon S3,
and sends an Amazon SNS notification after successful execution.
"""


BUCKET_NAME = os.environ["BUCKET_NAME"]
INPUT_OBJECT_KEY = os.environ.get("INPUT_OBJECT_KEY", "raw/atp_tennis.csv")
TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    sns = boto3.client("sns")

    # Read raw ATP data from S3
    obj = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=INPUT_OBJECT_KEY
    )

    csv_data = obj["Body"].read().decode("utf-8-sig")
    df = pd.read_csv(StringIO(csv_data))

    # Prepare date column
    df["Date"] = pd.to_datetime(df["Date"])

    # Generate player ranking
    ranking = (
        df.groupby("Winner")
        .agg(
            total_wins=("Winner", "count"),
            first_win=("Date", "min"),
            last_win=("Date", "max"),
            grand_slam_wins=("Series", lambda x: (x == "Grand Slam").sum()),
            atp1000_wins=("Series", lambda x: (x == "Masters 1000").sum()),
            atp500_wins=("Series", lambda x: (x == "ATP500").sum()),
        )
        .reset_index()
        .rename(columns={"Winner": "player_name"})
    )

    # Select Top 50 players based on total wins
    ranking = (
        ranking.sort_values(by="total_wins", ascending=False)
        .head(50)
    )

    ranking = ranking[
        [
            "player_name",
            "total_wins",
            "grand_slam_wins",
            "atp1000_wins",
            "atp500_wins",
            "first_win",
            "last_win",
        ]
    ]

    # Create dated output filename
    output_file_name = (
        f"final/atp-top-50-{datetime.now().strftime('%d-%m-%Y')}.csv"
    )

    csv_output = ranking.to_csv(index=False)

    # Save processed ranking to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=output_file_name,
        Body=csv_output.encode("utf-8-sig"),
        ContentType="text/csv",
    )

    # Send completion notification
    sns.publish(
        TopicArn=TOPIC_ARN,
        Subject="ATP Analysis Completed",
        Message=(
            "ATP analysis finished successfully.\n\n"
            f"Output file: {output_file_name}\n"
            f"Bucket: {BUCKET_NAME}\n"
            f"Time: {datetime.now()}"
        ),
    )
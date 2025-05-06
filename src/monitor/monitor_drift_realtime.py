from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, text
import pandas as pd

from datetime import datetime, timedelta
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently.column_mapping import ColumnMapping


def monitor_drift():
    # Time window: 7 ngày trước vs 7 ngày mới nhất
    today = datetime.today().date()
    start_current = today - timedelta(days=7)
    start_reference = today - timedelta(days=14)
    end_reference = start_current - timedelta(days=1)

    # Load data từ bảng forecasted
    reference_df = pd.read_sql(
        f"SELECT * FROM forecasted WHERE ds BETWEEN '{start_reference}' AND '{end_reference}'",
        engine
    )
    current_df = pd.read_sql(
        f"SELECT * FROM forecasted WHERE ds >= '{start_current}'",
        engine
    )

    # Define mapping
    column_mapping = ColumnMapping(
        numerical_features=["mean", "lo_ci", "hi_ci"],
        categorical_features=["unique_id"]
    )

    # Run Evidently
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference_df, current_data=current_df,
               column_mapping=column_mapping)

    drift_result = report.as_dict()
    drifted_columns = [
        col for col, val in drift_result["metrics"][0]["result"]["drift_by_columns"].items()
        if val["drift_detected"]
    ]

    # Ghi vào bảng drift_log trong PostgreSQL
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS drift_log (
                id SERIAL PRIMARY KEY,
                drifted_columns TEXT[],
                checked_at TIMESTAMP DEFAULT NOW()
            );
        """))
        conn.execute(
            text("INSERT INTO drift_log (drifted_columns) VALUES (:cols)"),
            {"cols": drifted_columns}
        )

    # Lưu HTML locally
    html_path = "./drift_report.html"
    report.save_html(html_path)

    return {
        "drifted_columns": drifted_columns,
        "report_path": "/static/drift_report.html"  # nếu gắn kèm HTML report sau này
    }

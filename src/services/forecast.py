# Databricks notebook source
import mlflow
from dotenv import load_dotenv
import os
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently.column_mapping import ColumnMapping

mlflow.autolog(disable=True)
mlflow.set_registry_uri("databricks-uc")

# Load env
load_dotenv()


def forecast_demand(df):
    model = mlflow.pyfunc.load_model(
        model_url=f"{os.getenv("BASE_URL")}.MAPIE-LGBM-regress-tweedie_2w1@Production"
    )
    prediction = model.predict(df)
    results = {
        "prediction_lower": prediction[:, 0].tolist(),
        "prediction": prediction[:, 1].tolist(),
        "prediction_upper": prediction[:, 2].tolist()
    }

    return results

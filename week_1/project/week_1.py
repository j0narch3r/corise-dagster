import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": str},
    description="Return a list of stock from S3",
    out={"stocks": Out(dagster_type=List, is_required=True, description="Outputs a list of stocks")}
)
def get_s3_data_op(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [*csv_helper(s3_key)]


@op(
    description="Returns the highest value stock given a list of stocks",
    out={"stock": Out(dagster_type=Aggregation, is_required=True, description="Highest value stock")}
)
def process_data_op(context, stocks: List) -> Aggregation:
    highest_stock_value = max(stocks, key = lambda k: k.high)
    context.log.info(f"Highest value stock: {highest_stock_value}")
    return Aggregation(date=highest_stock_value.date, high=highest_stock_value.high)


@op
def put_redis_data_op(context, stock: Aggregation):
    pass


@op
def put_s3_data_op(context, stock: Aggregation):
    pass


@job
def machine_learning_job():
    data = process_data_op(get_s3_data_op())
    put_redis_data_op(data)
    put_s3_data_op(data)
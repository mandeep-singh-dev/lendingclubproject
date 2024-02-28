import pytest
from lib.Utils import get_spark_session


@pytest.fixture
def spark():
    "creates spark session"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()
    
@pytest.fixture
def expected_results(spark):
    "gives expected results"
    results_schema = "state string, count int"
    df = spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("data/test_result/state_aggregate.csv")
    
    print("inside fixture expected results")
    print(df.show(truncate=False))
    
    return df

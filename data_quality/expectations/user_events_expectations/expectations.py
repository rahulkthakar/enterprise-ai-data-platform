# data_quality/expectations/user_events_expectations/expectations.py

import great_expectations as gx


def validate_events(df):

    # validator = gx.from_pandas(df.toPandas())

    # Convert Spark DF to Pandas
    pandas_df = df.toPandas()

    # 1. Get an ephemeral context
    context = gx.get_context()

    # 2. Define a data source and asset
    data_source = context.data_sources.add_pandas("my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset("my_batch_asset")

    # 3. Create a Batch Definition
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "my_batch_definition"
    )

    # 4. Get a Batch by passing the actual dataframe
    batch = batch_definition.get_batch(batch_parameters={"dataframe": pandas_df})

    # 5. Get the Validator FROM THE CONTEXT using the batch
    validator = context.get_validator(batch=batch)

    results = []

    # price > 0
    results.append(validator.expect_column_values_to_be_between("price", min_value=0))

    # timestamp not null
    results.append(validator.expect_column_values_to_not_be_null("timestamp"))

    # valid event_type
    results.append(
        validator.expect_column_values_to_be_in_set(
            "event_type", ["view", "cart", "purchase"]
        )
    )

    # success = all(r["success"] for r in results)
    success = all(r.success for r in results)

    return success

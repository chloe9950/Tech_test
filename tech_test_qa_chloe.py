import great_expectations as gx
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Run Great Expectations validation.")
parser.add_argument("--username", type=str, required=True, help="Database username")
parser.add_argument("--password", type=str, required=True, help="Database password")
parser.add_argument("--host", type=str, required=True, help="Database host")
parser.add_argument("--port", type=int, required=True, help="Database port")
parser.add_argument("--database", type=str, required=True, help="Database name")
args = parser.parse_args()

# Construct the database connection string
PG_CONNECTION_STRING = f"postgresql+psycopg2://{args.username}:{args.password}@{args.host}:{args.port}/{args.database}"

# Initialize Great Expectations context
context = gx.get_context()

pg_datasource = context.sources.add_postgres(
    name="pg_datasource", 
    connection_string=PG_CONNECTION_STRING
)

# ------------------------------------------------------------------------------
# USERS TABLE
# ------------------------------------------------------------------------------
users_asset = pg_datasource.add_table_asset(name="users", table_name="users")

context.add_or_update_expectation_suite(expectation_suite_name="users_suite")
users_validator = context.get_validator(
    batch_request=users_asset.build_batch_request(),
    expectation_suite_name="users_suite",
)

# Not null checks
users_validator.expect_column_values_to_not_be_null(column="login_hash")
users_validator.expect_column_values_to_not_be_null(column="server_hash")
users_validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)

# Check login_hash is hex-like
users_validator.expect_column_values_to_match_regex(
    column="login_hash", 
    regex=r"^[A-F0-9]+$"
)

# Check 'country_hash' is not null and also hex-like 
users_validator.expect_column_values_to_not_be_null(column="country_hash")
users_validator.expect_column_values_to_match_regex(
    column="country_hash", 
    regex=r"^[A-F0-9]+$"
)

# Check 'currency' is in an allowed set
users_validator.expect_column_values_to_not_be_null(column="currency")
users_validator.expect_column_values_to_be_in_set(
    column="currency",
    value_set=["AUD", "EUR", "NZD", "USD"]  
)

# 'enable' must be 0 or 1
users_validator.expect_column_values_to_not_be_null(column="enable")
users_validator.expect_column_values_to_be_in_set(
    column="enable",
    value_set=[0, 1]
)

users_server_hashes = users_validator.head(fetch_all=True)["server_hash"].drop_duplicates().tolist()
users_login_hashes = users_validator.head(fetch_all=True)["login_hash"].drop_duplicates().tolist()

users_validator.save_expectation_suite(discard_failed_expectations=False)

# ------------------------------------------------------------------------------
# TRADES TABLE
# ------------------------------------------------------------------------------
trades_asset = pg_datasource.add_table_asset(name="trades", table_name="trades")

context.add_or_update_expectation_suite(expectation_suite_name="trades_suite")
trades_validator = context.get_validator(
    batch_request=trades_asset.build_batch_request(),
    expectation_suite_name="trades_suite",
)

# Not null checks
trades_validator.expect_column_values_to_not_be_null(column="login_hash")
trades_validator.expect_column_values_to_not_be_null(column="symbol")
trades_validator.expect_column_values_to_not_be_null(column="contractsize")
trades_validator.expect_column_values_to_not_be_null(column="open_time")
trades_validator.expect_column_values_to_not_be_null(column="close_time")

# Symbol should be alphanumeric
trades_validator.expect_column_values_to_match_regex(
    column="symbol", 
    regex=r"^[A-Za-z0-9]+$"
)

# Validate numeric columns are >= 0 
trades_validator.expect_column_values_to_be_between(
    column="volume",
    min_value=0,  
    max_value=None
)

# Digits and cmd in valid range 
trades_validator.expect_column_values_to_be_between(
    column="digits",
    min_value=0,
    max_value=10
)

trades_validator.expect_column_values_to_be_in_set(
    column="cmd",
    value_set=[0, 1]
)

# ticket_hash not null & hex-like 
trades_validator.expect_column_values_to_not_be_null(column="ticket_hash")
trades_validator.expect_column_values_to_match_regex(
    column="ticket_hash", 
    regex=r"^[A-F0-9]+$"
)

# Validate for every row open_time < close_time 
trades_validator.expect_column_pair_values_a_to_be_greater_than_b(
    column_A="close_time",
    column_B="open_time"
)

# server_hash and login_hash must match what's in the users table
trades_validator.expect_column_values_to_be_in_set(
    column="server_hash", 
    value_set=users_server_hashes
)
trades_validator.expect_column_values_to_be_in_set(
    column="login_hash", 
    value_set=users_login_hashes
)

trades_validator.save_expectation_suite(discard_failed_expectations=False)

# ------------------------------------------------------------------------------
# CHECKPOINTS
# ------------------------------------------------------------------------------
users_checkpoint = gx.checkpoint.Checkpoint(
    name="users_checkpoint",
    data_context=context,
    batch_request=users_asset.build_batch_request(),
    expectation_suite_name="users_suite",
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"}
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"}
        },
    ],
)

trades_checkpoint = gx.checkpoint.Checkpoint(
    name="trades_checkpoint",
    data_context=context,
    batch_request=trades_asset.build_batch_request(),
    expectation_suite_name="trades_suite",
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"}
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"}
        },
    ],
)

users_checkpoint_result = users_checkpoint.run()
trades_checkpoint_result = trades_checkpoint.run()

context.open_data_docs()
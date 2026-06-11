from pystackt import get_github_log, export_to_ocel2

#please, insert your GITHUB_ACCESS_TOKEN
#Data collected on May 21, 2026
get_github_log(
    GITHUB_ACCESS_TOKEN="your_github_token",
    repo_owner="pydantic",
    repo_name="pydantic",
    max_issues=2500,
    save_after_num_issues=500,
    quack_db="ocel/pydantic.duckdb"
)

export_to_ocel2(
    quack_db="ocel/pydantic.duckdb",
    schema_in="main",
    schema_out="ocel2",
    sqlite_db="ocel/pydantic.sqlite"
)

print("Done!")

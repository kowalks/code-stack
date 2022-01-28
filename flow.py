from prefect import Flow

with Flow(
    "DBT Daily",
) as f:

    pull_repo = pull_dbt_repo()
    deps = dbt(
        command="dbt deps",
        task_args={"name": "DBT: Dependencies"},
        upstream_tasks=[pull_repo],
    )
    deps_output = output_print(
        deps,
        task_args={"name": "DBT: Dependency Output"},
    )
    run_daily = dbt(
        command="dbt run -m tag:daily",
        task_args={"name": "DBT: Run Dailies"},
        upstream_tasks=[pull_repo, deps_output],
    )
    output_print(run_daily, task_args={"name": "DBT Daily Output"})
    test = dbt(
        command="dbt test -m tag:daily",
        task_args={"name": "DBT: Test Dailies"},
        upstream_tasks=[pull_repo, run_daily],
    )
    output_print(test)

f.run()

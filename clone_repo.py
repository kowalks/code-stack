from prefect import task
from prefect.client import Secret
import pygit2


@task(name="Clone DBT")
def pull_dbt_repo():
    logger = prefect.context.get("logger")
    shutil.rmtree("dbt", ignore_errors=True)  # Delete folder on run
    git_token = Secret("GITHUB_ACCESS_TOKEN").get()
    dbt_repo_name = "slate-dbt"
    dbt_repo = (
        f"https://{git_token}:x-oauth-basic@github.com/slate-data/{dbt_repo_name}"
    )

    pygit2.clone_repository(dbt_repo, "dbt")


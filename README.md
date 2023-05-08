# prefect-lakefs

<p align="center">
    <!--- Insert a cover image here -->
    <!--- <br> -->
    <a href="https://pypi.python.org/pypi/prefect-lakefs/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-lakefs?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/limx0/prefect-lakefs/" alt="Stars">
        <img src="https://img.shields.io/github/stars/limx0/prefect-lakefs?color=0052FF&labelColor=090422" /></a>
    <a href="https://pypistats.org/packages/prefect-lakefs/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-lakefs?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/limx0/prefect-lakefs/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/limx0/prefect-lakefs?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

Visit the full docs [here](https://limx0.github.io/prefect-lakefs) to see additional examples and the API reference.

Prefect integrations for interacting with LakeFS services.


## Welcome!

`prefect-lakefs` is a collection of Prefect tasks and flows which can be used to interact with lakeFS managed datalakes.

Jump to [examples](#example-usage).


## Resources

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/collections/usage/)!

### Installation

Install `prefect-lakefs` with `pip`:

```bash
pip install prefect-lakefs
```

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

### Example Usage

#### Configure LakeFS Credentials and dive in.

```python
import asyncio

from prefect_lakefs.credentials import LakeFSCredentials
from prefect_lakefs.tasks import list_branches

# You can configure this while adding a block in the prefect-ui or 
#   you can save the block using .save() utility method provided by the block.
lakefs_creds = LakeFSCredentials(
        endpoint_url="http://localhost:8000/api/v1",
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    ).save("lakefs-creds")


@flow
def list_branches_for_example_repo():
    branches = list_branches(
        lakefs_credentials=LakeFSCredentials.load("lakefs-creds"),
        repository="example",
    )
    print(branches)
 


if __name__ == "__main__":
    # run the flow
    asyncio.run(list_branches())
```


### Feedback

If you encounter any bugs while using `prefect-lakefs`, feel free to open an issue in the [prefect-lakefs](https://github.com/limx0/prefect-lakefs) repository.

If you have any questions or issues while using `prefect-lakefs`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack) or the [LakeFS Slack Community](https://go.lakefs.io/JoinSlack).

Feel free to star or watch [`prefect-lakefs`](https://github.com/limx0/prefect-lakefs) for updates too!

### Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-lakefs`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/limx0/prefect-lakefs/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request

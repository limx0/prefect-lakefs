"""This is an example flows module"""
from prefect import flow

from prefect_lakefs.blocks import LakefsBlock
from prefect_lakefs.tasks import (
    goodbye_prefect_lakefs,
    hello_prefect_lakefs,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    LakefsBlock.seed_value_for_example()
    block = LakefsBlock.load("sample-block")

    print(hello_prefect_lakefs())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_lakefs())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()

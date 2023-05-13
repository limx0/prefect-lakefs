# from unittest.mock import patch
#
# from prefect import flow, task
#
# from prefect_lakefs.storage import LakeFS, LakeFSCredentials
#
#
# @patch("prefect_lakefs.storage.LakeFS._write_sync")
# def test_storage_init(mock_write_sync):
#     @task(
#         result_storage=LakeFS(
#             bucket_name="bucket_name", credentials=LakeFSCredentials()
#         ),
#         persist_result=True,
#     )
#     def my_task():
#         return 1
#
#     @flow
#     def my_flow():
#         my_task()
#
#     my_flow()
#
#     assert mock_write_sync.call_count == 1

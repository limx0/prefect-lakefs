from lakefs_client.models import ObjectStageCreation, RefsDump

from prefect_lakefs.internal import dump_refs, restore_refs, stage_object


async def test_stage_object(lakefs_credentials, _mock_internal_client):
    await stage_object.fn(
        repository="example",
        ref="main",
        path="obj/path",
        physical_address="node_addr",
        checksum="checksum_hash",
        size_bytes=11,
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_internal_client.stage_object.call_args[1]["repository"] == "example"
    assert _mock_internal_client.stage_object.call_args[1]["ref"] == "main"
    assert _mock_internal_client.stage_object.call_args[1]["path"] == "obj/path"
    assert _mock_internal_client.stage_object.call_args[1][
        "object_stage_creation"
    ] == ObjectStageCreation(
        physical_address="node_addr",
        checksum="checksum_hash",
        size_bytes=11,
    )


async def test_dump_refs(lakefs_credentials, _mock_internal_client):
    await dump_refs.fn(
        repository="example",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_internal_client.dump_refs.call_args[1]["repository"] == "example"


async def test_restore_refs(lakefs_credentials, _mock_internal_client):
    await restore_refs.fn(
        repository="example",
        refs_dump=RefsDump(
            commits_meta_range_id="commits_meta_range_id",
            tags_meta_range_id="tags_meta_range_id",
            branches_meta_range_id="branches_meta_range_id",
        ),
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_internal_client.restore_refs.call_args[1]["repository"] == "example"

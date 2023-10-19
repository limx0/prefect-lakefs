from lakefs_client.models import ObjectCopyCreation, PathList

from prefect_lakefs.objects import (
    copy_object,
    delete_object,
    delete_objects,
    get_object,
    get_underlying_properties,
    head_object,
    list_objects,
    stat_object,
    upload_object,
)


async def test_copy_object(lakefs_credentials, _mock_objects_client):
    await copy_object.fn(
        repository="example",
        branch="main",
        dest_path="dest/path",
        src_path="src/path",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.copy_object.call_args[1]["repository"] == "example"
    assert _mock_objects_client.copy_object.call_args[1]["branch"] == "main"
    assert _mock_objects_client.copy_object.call_args[1]["dest_path"] == "dest/path"
    assert _mock_objects_client.copy_object.call_args[1][
        "object_copy_creation"
    ] == ObjectCopyCreation(
        src_path="src/path",
        src_ref="main",
    )


async def test_delete_object(lakefs_credentials, _mock_objects_client):
    await delete_object.fn(
        repository="example",
        branch="main",
        path="path",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.delete_object.call_args[1]["repository"] == "example"
    assert _mock_objects_client.delete_object.call_args[1]["branch"] == "main"
    assert _mock_objects_client.delete_object.call_args[1]["path"] == "path"


async def test_delete_objects(lakefs_credentials, _mock_objects_client):
    await delete_objects.fn(
        repository="example",
        branch="main",
        path_list=["path_0", "path_1"],
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.delete_objects.call_args[1]["repository"] == "example"
    assert _mock_objects_client.delete_objects.call_args[1]["branch"] == "main"
    assert _mock_objects_client.delete_objects.call_args[1]["path_list"] == PathList(
        paths=[
            "path_0",
            "path_1",
        ]
    )


async def test_get_object(lakefs_credentials, _mock_objects_client):
    await get_object.fn(
        repository="example",
        ref="main",
        path="obj/path",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.get_object.call_args[1]["repository"] == "example"
    assert _mock_objects_client.get_object.call_args[1]["ref"] == "main"
    assert _mock_objects_client.get_object.call_args[1]["path"] == "obj/path"


async def test_get_underlying_properties(lakefs_credentials, _mock_objects_client):
    await get_underlying_properties.fn(
        repository="example",
        ref="main",
        path="obj/path",
        lakefs_credentials=lakefs_credentials,
    )
    assert (
        _mock_objects_client.get_underlying_properties.call_args[1]["repository"]
        == "example"
    )
    assert _mock_objects_client.get_underlying_properties.call_args[1]["ref"] == "main"
    assert (
        _mock_objects_client.get_underlying_properties.call_args[1]["path"]
        == "obj/path"
    )


async def test_head_object(lakefs_credentials, _mock_objects_client):
    await head_object.fn(
        repository="example",
        ref="main",
        path="obj/path",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.head_object.call_args[1]["repository"] == "example"
    assert _mock_objects_client.head_object.call_args[1]["ref"] == "main"
    assert _mock_objects_client.head_object.call_args[1]["path"] == "obj/path"


async def test_list_objects(lakefs_credentials, _mock_objects_client):
    await list_objects.fn(
        repository="example",
        ref="main",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.list_objects.call_args[1]["repository"] == "example"
    assert _mock_objects_client.list_objects.call_args[1]["ref"] == "main"


async def test_stat_object(lakefs_credentials, _mock_objects_client):
    await stat_object.fn(
        repository="example",
        ref="main",
        path="obj/path",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.stat_object.call_args[1]["repository"] == "example"
    assert _mock_objects_client.stat_object.call_args[1]["ref"] == "main"
    assert _mock_objects_client.stat_object.call_args[1]["path"] == "obj/path"


async def test_upload_object(lakefs_credentials, _mock_objects_client):
    await upload_object.fn(
        repository="example",
        branch="main",
        path="obj/path",
        lakefs_credentials=lakefs_credentials,
    )
    assert _mock_objects_client.upload_object.call_args[1]["repository"] == "example"
    assert _mock_objects_client.upload_object.call_args[1]["branch"] == "main"
    assert _mock_objects_client.upload_object.call_args[1]["path"] == "obj/path"

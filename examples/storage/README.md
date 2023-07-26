# Storage example

This example shows how to use LakeFS as a result storage backend.


# Setup

- LakeFS, minio and postgres via the docker-compose file:

```bash
docker-compose up -d
```

Minio:
- Go to the Minio UI at http://localhost:9001/login and login with credentials from docker-compose:
  - user: `minioadmin`
  - password: `minioadmin`
- Create a bucket called `lakefs`

LakeFS:
- Go to the LakeFS UI at http://127.0.0.1:8000/setup and setup a user and email
- Record the `Access Key ID` and secret key `Secret Access Key` somewhere.
- Go to login and login with access key id and secret access key.
- Create a sample repository using `s3://lakefs` as the Storage Namespace.

Run:
- Open the `storage_example.ipynb` notebook and run it.

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
- Go to the LakeFS UI at http://127.0.0.1:8000/setup and setup a user and email.
- Download credentials and place in the current directory.
- Go to login and login with access key id and secret access key from credential file.
- Create a sample repository:
  - `Repository ID`: lakefs
  - `Storage Namespace`: `s3://lakefs`
  - `Default Branch`: `main`

# Run:
- Run the `example.py` file to push some data to lakefs.
- `git checkout -b mynewbranch`
- Make changes to `example.py`
- Re-run the `example.py` file to push changes to a new branch in lakefs automatically.
-

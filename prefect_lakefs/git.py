from typing import Optional

from git import Repo


def get_git_repo() -> Repo:
    return Repo(search_parent_directories=True)


def commit_sha(repo: Optional[Repo] = None) -> str:
    repo = repo or get_git_repo()
    return repo.commit().hexsha


def branch_name(repo: Optional[Repo] = None) -> str:
    repo = repo or get_git_repo()
    return repo.active_branch.name

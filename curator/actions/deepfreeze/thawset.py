from dataclasses import dataclass


@dataclass
class ThawedRepo:
    """
    Data class for a thawed repo and indices
    """

    repo_name: str
    bucket_name: str
    base_path: str
    provider: str
    indices: list = None

    def __init__(self, repo_info: dict, indices: list[str] = None) -> None:
        self.repo_name = repo_info["name"]
        self.bucket_name = repo_info["bucket"]
        self.base_path = repo_info["base_path"]
        self.provider = "aws"
        self.indices = indices

    def add_index(self, index: str) -> None:
        """
        Add an index to the list of indices

        :param index: The index to add
        """
        self.indices.append(index)


class ThawSet(dict[str, ThawedRepo]):
    """
    Data class for thaw settings
    """

    def add(self, thawed_repo: ThawedRepo) -> None:
        """
        Add a thawed repo to the dictionary

        :param thawed_repo: A thawed repo object
        """
        self[thawed_repo.repo_name] = thawed_repo

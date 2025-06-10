import shutil
from pathlib import Path

import attrs


@attrs.define
class LocalFileSystem:
    def list(self, root: str, pattern: str = "*") -> list[str]:
        return list(Path(root).glob(pattern))

    def copy(self, src: str, dst: str) -> bool:
        try:
            Path(dst).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dst)
            return True  # noqa: TRY300
        except OSError as e:
            raise e from e

    def move(self, src: str, dst: str) -> bool:
        try:
            Path(dst).parent.mkdir(parents=True, exist_ok=True)
            shutil.move(src, dst)
            return True  # noqa: TRY300
        except OSError as e:
            raise e from e

    def delete(self, path: str) -> bool:
        try:
            path = Path(path)
            if path.is_dir():
                shutil.rmtree(path)
            elif path.is_file():
                Path(path).unlink()
            else:
                return False
            return True  # noqa: TRY300
        except FileExistsError as e:
            raise e from e

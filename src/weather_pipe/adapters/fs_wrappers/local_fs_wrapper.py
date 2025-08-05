import shutil
from pathlib import Path

import attrs

from weather_pipe.domain.result import safe


@attrs.define
class LocalFileSystem:
    def list(self, root: str, pattern: str = "*") -> list[str]:
        return list(Path(root).glob(pattern))

    @safe
    def copy(self, src: str, dst: str) -> bool:
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dst)
        return True

    @safe
    def move(self, src: str, dst: str) -> bool:
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(src, dst)
        return True

    @safe
    def delete(self, path: str) -> bool:
        path = Path(path)
        if path.is_dir():
            shutil.rmtree(path)
        elif path.is_file():
            Path(path).unlink()
        else:
            return False
        return True

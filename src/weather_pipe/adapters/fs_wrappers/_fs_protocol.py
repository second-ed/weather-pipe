from copy import deepcopy
from typing import Protocol, runtime_checkable

import attrs


@runtime_checkable
class FileSystemProtocol(Protocol):
    def list(self, root: str) -> list[str]: ...

    def copy(self, src: str, dst: str) -> bool: ...

    def move(self, src: str, dst: str) -> bool: ...

    def delete(self, path: str) -> bool: ...


@attrs.define
class FakeFileSystem:
    db: dict = attrs.field(default=attrs.Factory(dict))
    log: list = attrs.field(default=attrs.Factory(list))

    def list(self, root: str, pattern: str = "*") -> list[str]:
        self.log.append({"func": "list", "root": root, "pattern": pattern})
        return [key for key in self.db if key.startswith(root)]

    def copy(self, src: str, dst: str) -> bool:
        self.log.append({"func": "copy", "src": src, "dst": dst})
        if src in self.db:
            self.db[dst] = deepcopy(self.db[src])
            return True
        return False

    def move(self, src: str, dst: str) -> bool:
        self.log.append({"func": "move", "src": src, "dst": dst})
        if src in self.db:
            self.db[dst] = self.db.pop(src)
            return True
        return False

    def delete(self, path: str) -> bool:
        self.log.append({"func": "delete", "path": path})
        return self.db.pop(path, None) is not None

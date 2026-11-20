"""Script text of one write operation as RxScript.Write composes it (src/Runwick.Redis/Store/RxClient.cs): the generic frame (prelude,
jobs.lua, the op body) and, for the worker operations with a lean path, that frame wrapped as GENERIC() ahead of lean/frame.lua and the
lean body. A Lua directory without lean/ (an older baseline) gives the generic text."""
from pathlib import Path

LEAN = {"reserve_and_begin", "finish", "finish_many", "renew"}


def generic(lua, op):
    lua = Path(lua)
    return (lua / "prelude.lua").read_text() + "\n" + (lua / "jobs.lua").read_text() + "\n" + (lua / f"ops/{op}.lua").read_text()


def compose(lua, op, lean=True):
    lua = Path(lua)
    if lean and op in LEAN and (lua / "lean" / f"{op}.lua").exists():
        return ("local function GENERIC()\n" + generic(lua, op) + "\nend\n" + (lua / "lean/frame.lua").read_text() + "\n"
                + (lua / f"lean/{op}.lua").read_text())
    return generic(lua, op)

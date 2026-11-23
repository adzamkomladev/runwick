"""Script text of one write operation as RxScript.Write composes it (src/Runwick.Redis/Store/RxClient.cs): the generic frame (prelude,
jobs.lua, the op body) and, for the worker operations with a lean path, that frame wrapped as GENERIC() ahead of lean/frame.lua and the
lean body. A Lua directory without lean/ (an older baseline) gives the generic text."""
import hashlib, re
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


def library(lua, wrap=None):
    """The Redis Functions library of the lean operations as RxScript.Library composes it: (library name, text). wrap(op, body) may
    replace each lean body (the profile tool times it); the name is then that of the wrapped text."""
    lua = Path(lua)
    ops = sorted(LEAN)
    body = "local KEYS, ARGV\nlocal G = {}\n"
    for op in ops:
        body += f"G['{op}'] = function()\n" + generic(lua, op) + "\nend\n"
    body += (lua / "lean/frame.lua").read_text() + "\n"
    for op in ops:
        lean = (lua / f"lean/{op}.lua").read_text()
        body += (f"do\nlocal GENERIC = G['{op}']\nredis.register_function('{{LIB}}_{op}', function(keys, args)\nKEYS, ARGV = keys, args\n"
                 + (wrap(op, lean) if wrap else lean) + "\nend)\nend\n")
    name = "rw_" + hashlib.sha1(body.encode()).hexdigest()[:16]
    return name, "#!lua name=" + name + "\n" + body.replace("{LIB}", name)


def read(lua, op):
    """A read-only script (RxScript.Read): prelude, jobs.lua and the op body."""
    return "#!lua flags=no-writes\n" + generic(lua, op)


def layout(lua):
    """The layout revision the directory's scripts accept (the gate in prelude.lua): 1 on main before 2026-11-20, 2 after."""
    return int(re.search(r"m\.layout ~= '(\d+)'", (Path(lua) / "prelude.lua").read_text()).group(1))


def hint(lua):
    """The per-item hint argument of FinishMany/FinishAndClaim items ([''] when the scripts take one, else [])."""
    return [""] if "hint" in (Path(lua) / "ops/finish_many.lua").read_text() else []

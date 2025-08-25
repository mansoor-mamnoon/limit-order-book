import os
import shutil
import subprocess
import sys
from pathlib import Path
import click


@click.group(help="LOB utilities")
def cli() -> None:
    """Root command group."""
    pass


def _existing(p: Path) -> str | None:
    return str(p) if p.is_file() and os.access(p, os.X_OK) else None


def _find_bench_tool() -> str | None:
    # 1) PATH
    exe = shutil.which("bench_tool")
    if exe:
        return exe

    # 2) venv bin/Scripts
    venv_bin = Path(sys.prefix) / ("Scripts" if os.name == "nt" else "bin")
    exe = _existing(venv_bin / ("bench_tool.exe" if os.name == "nt" else "bench_tool"))
    if exe:
        return exe

    # 3) scikit-build editable locations + common local builds
    here = Path(__file__).resolve()
    repo = here.parents[3] if (len(here.parents) >= 4 and (here.parents[3] / "cpp").exists()) else here.parents[2]
    candidates: list[Path] = []

    # a) _skbuild/**/bench_tool
    for p in repo.glob("_skbuild/*/*"):
        if p.is_dir():
            candidates.extend(p.rglob("bench_tool"))

    # b) common build dirs
    candidates.append(repo / "build" / "cpp" / "bench_tool")
    candidates.append(repo / "cpp" / "build" / "bench_tool")

    # c) rare: bundled next to package
    candidates.append(here.parent / "bench_tool")

    for c in candidates:
        exe = _existing(c)
        if exe:
            return exe

    # 4) user override
    env = os.getenv("LOB_BENCH")
    if env and _existing(Path(env)):
        return env

    return None


@cli.command("bench", help="Run the native C++ bench tool with --msgs.")
@click.option("--msgs", type=float, default=1e6, show_default=True, help="Number of messages")
def bench(msgs: float) -> None:
    exe = _find_bench_tool()
    if not exe:
        click.secho(
            "bench_tool not found.\n"
            "Fix options:\n"
            "  1) Build it:  cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release && cmake --build build/cpp -j\n"
            "  2) Or set LOB_BENCH=/full/path/to/bench_tool and rerun.\n",
            fg="red",
        )
        raise click.Abort()

    n = str(int(msgs))
    # Try the most common CLI shapes used by benchmark tools
    trials = [
        [exe, "--msgs", n],
        [exe, "--num", n],
        [exe, "-n", n],
        [exe, n],  # positional
    ]

    last_err: subprocess.CalledProcessError | None = None
    for args in trials:
        try:
            subprocess.check_call(args)
            return
        except subprocess.CalledProcessError as e:
            last_err = e
            continue

    # If we reach here, all shapes failed
    click.secho("bench_tool failed with all known argument forms:", fg="red")
    for a in trials:
        click.echo("  $ " + " ".join(a))
    if last_err is not None:
        click.echo(f"\nLast error code: {last_err.returncode}")
    click.echo("\nTry running the tool manually to see its usage/help:")
    click.echo(f"  $ {exe} --help  (or)  $ {exe}")
    raise click.Abort()


if __name__ == "__main__":
    cli()

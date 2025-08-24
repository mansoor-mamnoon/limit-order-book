import shutil
import subprocess
from pathlib import Path
import typer

app = typer.Typer(help="LOB utilities")

@app.command()
def bench(msgs: float = typer.Option(1e6, "--msgs", help="Number of messages")):
    """
    Run the native C++ bench tool with --msgs.
    """
    exe = shutil.which("bench_tool")
    if not exe:
        # Fallback to a local build path (useful during dev before install)
        local = Path("build/cpp/bench_tool")
        if local.exists():
            exe = str(local)
    if not exe:
        typer.secho("bench_tool not found. Build or install first.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    subprocess.check_call([exe, "--msgs", str(int(msgs))])

def main():
    app()

if __name__ == "__main__":
    main()

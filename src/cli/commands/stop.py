"""Stop command for CDC services."""

import subprocess

import click
from rich.console import Console

console = Console()


@click.command()
@click.option("--remove-volumes", "-v", is_flag=True, help="Remove volumes")
def stop(remove_volumes: bool) -> None:
    """Stop CDC demo services."""
    console.print("\n[bold blue]Stopping CDC Demo Services[/bold blue]\n")

    cmd = ["docker-compose", "down"]
    if remove_volumes:
        cmd.append("-v")

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        console.print("[green]✓ Services stopped[/green]\n")
    except subprocess.CalledProcessError as e:
        console.print(f"[red]✗ Failed to stop services: {e.stderr}[/red]")
        raise click.Abort()

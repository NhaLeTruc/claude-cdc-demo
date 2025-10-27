"""Cleanup command for removing data and stopping services."""

import subprocess

import click
from rich.console import Console
from rich.prompt import Confirm

console = Console()


@click.command()
@click.option("--volumes", "-v", is_flag=True, help="Remove Docker volumes")
@click.option("--data", "-d", is_flag=True, help="Remove generated data files")
@click.option("--all", "-a", is_flag=True, help="Remove everything (volumes + data)")
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt")
def cleanup(volumes: bool, data: bool, all: bool, force: bool) -> None:
    """
    Stop services and clean up resources.

    By default, stops services without removing data.
    """
    console.print("\n[bold blue]CDC Demo Cleanup[/bold blue]\n")

    # Determine what to clean
    remove_volumes = volumes or all
    remove_data = data or all

    # Confirmation prompt
    if not force:
        message = "This will stop all services"
        if remove_volumes:
            message += " and remove all Docker volumes (database data will be lost)"
        if remove_data:
            message += " and remove generated data files"
        message += ". Continue?"

        if not Confirm.ask(message):
            console.print("[yellow]Cleanup cancelled[/yellow]\n")
            return

    try:
        # Stop services
        console.print("[cyan]Stopping services...[/cyan]")
        cmd = ["docker-compose", "down"]

        if remove_volumes:
            cmd.append("-v")

        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        console.print("[green]✓ Services stopped[/green]")

        # Remove data files
        if remove_data:
            console.print("[cyan]Removing data files...[/cyan]")
            subprocess.run(
                ["rm", "-rf", "data/", "*.parquet", "*.csv"],
                check=False,
                shell=True
            )
            console.print("[green]✓ Data files removed[/green]")

        # Clean Python artifacts
        console.print("[cyan]Cleaning Python artifacts...[/cyan]")
        subprocess.run(
            ["find", ".", "-type", "d", "-name", "__pycache__", "-exec", "rm", "-rf", "{}", "+"],
            check=False
        )
        subprocess.run(
            ["rm", "-rf", ".pytest_cache", ".coverage", "htmlcov"],
            check=False
        )
        console.print("[green]✓ Python artifacts cleaned[/green]")

        console.print("\n[bold green]✓ Cleanup complete![/bold green]\n")

    except subprocess.CalledProcessError as e:
        console.print(f"[red]✗ Cleanup failed: {e.stderr}[/red]\n")
        raise click.Abort()
    except Exception as e:
        console.print(f"[red]✗ Cleanup failed: {e}[/red]\n")
        raise click.Abort()

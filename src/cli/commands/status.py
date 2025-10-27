"""Status command for checking service health."""

import subprocess

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.command()
def status() -> None:
    """Check status of CDC demo services."""
    console.print("\n[bold blue]CDC Demo Service Status[/bold blue]\n")

    try:
        result = subprocess.run(
            ["docker-compose", "ps"], capture_output=True, text=True, check=True
        )

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Service")
        table.add_column("Status")
        table.add_column("Ports")

        # Parse docker-compose ps output
        lines = result.stdout.strip().split("\n")
        for line in lines[1:]:  # Skip header
            if line.strip():
                parts = line.split()
                if len(parts) >= 3:
                    service = parts[0]
                    status_text = parts[1] if "Up" in line else "Down"
                    status_color = "green" if "Up" in line else "red"
                    ports = " ".join(parts[2:]) if len(parts) > 2 else "-"

                    table.add_row(
                        service, f"[{status_color}]{status_text}[/{status_color}]", ports
                    )

        console.print(table)
        console.print()

    except subprocess.CalledProcessError as e:
        console.print(f"[red]✗ Failed to get status: {e.stderr}[/red]")
        raise click.Abort()

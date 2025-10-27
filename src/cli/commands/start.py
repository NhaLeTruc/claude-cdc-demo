"""Start command for launching CDC services."""

import subprocess
import time

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


@click.command()
@click.option("--detach/--no-detach", "-d", default=True, help="Run in detached mode")
@click.option(
    "--wait", is_flag=True, default=True, help="Wait for services to be healthy"
)
@click.option("--service", "-s", multiple=True, help="Start specific service(s)")
def start(detach: bool, wait: bool, service: tuple) -> None:
    """
    Start CDC demo services via docker-compose.

    By default, starts all services in detached mode and waits for them
    to become healthy.
    """
    console.print("\n[bold blue]Starting CDC Demo Services[/bold blue]\n")

    cmd = ["docker-compose", "up"]
    if detach:
        cmd.append("-d")
    if service:
        cmd.extend(service)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Starting services...", total=None)

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            progress.update(task, completed=True)

        console.print("[green]✓ Services started[/green]")

        if detach and wait:
            console.print("\n[yellow]Waiting for services to be healthy...[/yellow]")
            time.sleep(5)  # Give services time to start

            # Check service status
            _check_service_health()

        console.print("\n[bold green]✓ CDC Demo is running![/bold green]")
        console.print("\nAccess the services at:")
        console.print("  • Grafana: [link]http://localhost:3000[/link] (admin/admin)")
        console.print("  • Prometheus: [link]http://localhost:9090[/link]")
        console.print("  • MinIO Console: [link]http://localhost:9001[/link] (minioadmin/minioadmin)")
        console.print("  • Debezium: [link]http://localhost:8083[/link]")
        console.print("\nRun [cyan]cdc-demo status[/cyan] to check service health\n")

    except subprocess.CalledProcessError as e:
        console.print(f"[red]✗ Failed to start services: {e.stderr}[/red]")
        raise click.Abort()


def _check_service_health() -> None:
    """Check health of started services."""
    try:
        result = subprocess.run(
            ["docker-compose", "ps"], capture_output=True, text=True, check=True
        )

        # Count running services
        lines = result.stdout.strip().split("\n")[1:]  # Skip header
        running = sum(1 for line in lines if "Up" in line)

        console.print(f"[green]{running} services are running[/green]")

    except subprocess.CalledProcessError:
        console.print("[yellow]Could not verify service status[/yellow]")

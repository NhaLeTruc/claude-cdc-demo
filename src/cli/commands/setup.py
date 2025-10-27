"""Setup command for initializing the CDC demo."""

import subprocess
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


@click.command()
@click.option("--skip-build", is_flag=True, help="Skip Docker image build")
@click.option("--skip-install", is_flag=True, help="Skip Python dependencies installation")
def setup(skip_build: bool, skip_install: bool) -> None:
    """
    Initialize the CDC demo environment.

    This command:
    1. Checks prerequisites (Docker, Poetry)
    2. Installs Python dependencies
    3. Creates .env file if missing
    4. Builds Docker images
    """
    console.print("\n[bold blue]CDC Demo Setup[/bold blue]\n")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        # Check prerequisites
        task = progress.add_task("Checking prerequisites...", total=None)
        if not _check_docker():
            console.print("[red]✗ Docker is not installed or not running[/red]")
            raise click.Abort()
        if not _check_poetry() and not skip_install:
            console.print("[red]✗ Poetry is not installed[/red]")
            raise click.Abort()
        progress.update(task, completed=True)
        console.print("[green]✓ Prerequisites check passed[/green]")

        # Create .env file
        task = progress.add_task("Creating .env file...", total=None)
        _create_env_file()
        progress.update(task, completed=True)
        console.print("[green]✓ .env file ready[/green]")

        # Install Python dependencies
        if not skip_install:
            task = progress.add_task("Installing Python dependencies...", total=None)
            try:
                subprocess.run(
                    ["poetry", "install"], check=True, capture_output=True, text=True
                )
                progress.update(task, completed=True)
                console.print("[green]✓ Python dependencies installed[/green]")
            except subprocess.CalledProcessError as e:
                console.print(f"[red]✗ Failed to install dependencies: {e.stderr}[/red]")
                raise click.Abort()

        # Build Docker images
        if not skip_build:
            task = progress.add_task("Building Docker images...", total=None)
            try:
                subprocess.run(
                    ["docker-compose", "build"], check=True, capture_output=True, text=True
                )
                progress.update(task, completed=True)
                console.print("[green]✓ Docker images built[/green]")
            except subprocess.CalledProcessError as e:
                console.print(f"[red]✗ Failed to build images: {e.stderr}[/red]")
                raise click.Abort()

    console.print("\n[bold green]✓ Setup complete![/bold green]")
    console.print("\nNext steps:")
    console.print("  1. Run [cyan]cdc-demo start[/cyan] to start all services")
    console.print("  2. Run [cyan]cdc-demo status[/cyan] to check service status")
    console.print("  3. Run [cyan]cdc-demo generate customers[/cyan] to generate test data\n")


def _check_docker() -> bool:
    """Check if Docker is installed and running."""
    try:
        subprocess.run(
            ["docker", "info"], check=True, capture_output=True, text=True, timeout=5
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _check_poetry() -> bool:
    """Check if Poetry is installed."""
    try:
        subprocess.run(
            ["poetry", "--version"], check=True, capture_output=True, text=True, timeout=5
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return False


def _create_env_file() -> None:
    """Create .env file from .env.example if it doesn't exist."""
    env_file = Path(".env")
    example_file = Path(".env.example")

    if not env_file.exists() and example_file.exists():
        env_file.write_text(example_file.read_text())
        console.print("[yellow]Created .env from .env.example[/yellow]")
    elif env_file.exists():
        console.print("[yellow].env file already exists[/yellow]")
    else:
        console.print("[yellow]Warning: .env.example not found[/yellow]")

"""Test command for running test suites."""

import subprocess

import click
from rich.console import Console

console = Console()


@click.command()
@click.argument(
    "suite",
    type=click.Choice(["unit", "integration", "e2e", "data-quality", "all"]),
    default="all"
)
@click.option("--coverage", "-c", is_flag=True, help="Generate coverage report")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option("--marker", "-m", help="Run tests with specific marker")
def test(suite: str, coverage: bool, verbose: bool, marker: str | None) -> None:
    """
    Run test suites.

    SUITE can be: unit, integration, e2e, data-quality, or all
    """
    console.print(f"\n[bold blue]Running {suite} tests[/bold blue]\n")

    # Build pytest command
    cmd = ["poetry", "run", "pytest"]

    # Add test path based on suite
    if suite != "all":
        test_path = f"tests/{suite.replace('-', '_')}"
        cmd.append(test_path)

    # Add marker filter
    if marker:
        cmd.extend(["-m", marker])
    elif suite != "all":
        # Use suite name as marker
        marker_name = suite.replace("-", "_")
        cmd.extend(["-m", marker_name])

    # Add coverage
    if coverage:
        cmd.extend(["--cov=src", "--cov-report=html", "--cov-report=term-missing"])

    # Add verbose
    if verbose:
        cmd.append("-v")

    try:
        console.print(f"[dim]Running: {' '.join(cmd)}[/dim]\n")

        result = subprocess.run(cmd, check=False)

        if result.returncode == 0:
            console.print("\n[bold green]✓ All tests passed![/bold green]\n")

            if coverage:
                console.print("[cyan]Coverage report generated: htmlcov/index.html[/cyan]\n")
        else:
            console.print("\n[bold red]✗ Some tests failed![/bold red]\n")
            raise click.Abort()

    except FileNotFoundError:
        console.print("[red]✗ Poetry not found. Please install Poetry first.[/red]\n")
        raise click.Abort()
    except Exception as e:
        console.print(f"[red]✗ Test execution failed: {e}[/red]\n")
        raise click.Abort()

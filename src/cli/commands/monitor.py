"""Monitor command for observability dashboards."""

import subprocess
import webbrowser

import click
from rich.console import Console

console = Console()


@click.command()
@click.argument(
    "target",
    type=click.Choice(["grafana", "prometheus", "minio", "debezium", "all"]),
    default="grafana"
)
def monitor(target: str) -> None:
    """
    Open monitoring dashboards in browser.

    TARGET can be: grafana, prometheus, minio, debezium, or all
    """
    console.print(f"\n[bold blue]Opening {target} dashboard[/bold blue]\n")

    urls = {
        "grafana": "http://localhost:3000",
        "prometheus": "http://localhost:9090",
        "minio": "http://localhost:9001",
        "debezium": "http://localhost:8083"
    }

    try:
        if target == "all":
            for name, url in urls.items():
                console.print(f"[cyan]Opening {name}: {url}[/cyan]")
                _open_url(url)
        else:
            url = urls[target]
            console.print(f"[cyan]Opening {url}[/cyan]")
            _open_url(url)

        console.print("\n[green]✓ Dashboard(s) opened in browser[/green]")

        # Display credentials
        console.print("\n[bold]Default Credentials:[/bold]")
        console.print("  Grafana: admin / admin")
        console.print("  MinIO: minioadmin / minioadmin")
        console.print()

    except Exception as e:
        console.print(f"[red]✗ Failed to open dashboard: {e}[/red]")
        console.print(f"\n[dim]Please manually open: {urls.get(target, 'N/A')}[/dim]\n")


def _open_url(url: str) -> None:
    """Open URL in browser with fallback methods."""
    try:
        webbrowser.open(url)
    except Exception:
        # Try command line methods
        try:
            subprocess.run(["xdg-open", url], check=False)
        except Exception:
            try:
                subprocess.run(["open", url], check=False)
            except Exception:
                pass  # Silent fail, user can open manually

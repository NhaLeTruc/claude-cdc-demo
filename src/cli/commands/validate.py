"""Validate command for data integrity and CDC lag checking."""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.command()
@click.argument("target", type=click.Choice(["integrity", "lag", "schema", "all"]), default="all")
@click.option("--pipeline", "-p", help="Specific pipeline to validate")
@click.option("--threshold", "-t", type=int, help="Lag threshold in seconds")
def validate(target: str, pipeline: str | None, threshold: int | None) -> None:
    """
    Run validation checks on CDC pipelines.

    TARGET can be: integrity, lag, schema, or all
    """
    console.print(f"\n[bold blue]Running {target} validation[/bold blue]\n")

    try:
        from src.validation.orchestrator import ValidationOrchestrator

        orchestrator = ValidationOrchestrator()

        # Determine which validations to run
        validations = []
        if target in ["integrity", "all"]:
            validations.append("integrity")
        if target in ["lag", "all"]:
            validations.append("lag")
        if target in ["schema", "all"]:
            validations.append("schema")

        # Run validations
        results = {}
        for validation_type in validations:
            console.print(f"[cyan]Running {validation_type} validation...[/cyan]")

            if validation_type == "integrity":
                result = orchestrator.validate_integrity(pipeline_name=pipeline)
            elif validation_type == "lag":
                result = orchestrator.validate_lag(
                    pipeline_name=pipeline,
                    threshold_seconds=threshold
                )
            elif validation_type == "schema":
                result = orchestrator.validate_schema(pipeline_name=pipeline)
            else:
                continue

            results[validation_type] = result

        # Display results
        _display_results(results)

        # Check if any validations failed
        all_passed = all(
            r.overall_status.value == "passed"
            for r in results.values()
        )

        if all_passed:
            console.print("\n[bold green]✓ All validations passed![/bold green]\n")
        else:
            console.print("\n[bold red]✗ Some validations failed![/bold red]\n")
            raise click.Abort()

    except ImportError:
        console.print("[yellow]⚠ Validation orchestrator not fully implemented[/yellow]")
        console.print("[dim]Validation checks will be available after implementation[/dim]\n")
    except Exception as e:
        console.print(f"[red]✗ Validation failed: {e}[/red]")
        raise click.Abort()


def _display_results(results: dict) -> None:
    """Display validation results in a table."""
    for validation_type, report in results.items():
        table = Table(
            show_header=True,
            header_style="bold magenta",
            title=f"{validation_type.title()} Validation Results"
        )
        table.add_column("Validator")
        table.add_column("Status")
        table.add_column("Message")

        for result in report.results:
            status_color = {
                "passed": "green",
                "failed": "red",
                "warning": "yellow",
                "skipped": "dim"
            }.get(result.status.value, "white")

            table.add_row(
                result.validator,
                f"[{status_color}]{result.status.value}[/{status_color}]",
                result.message
            )

        console.print(table)
        console.print()

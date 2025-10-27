"""Main CLI entry point for CDC Demo."""

import click
from rich.console import Console

from src.cli.commands.cleanup import cleanup
from src.cli.commands.generate import generate
from src.cli.commands.monitor import monitor
from src.cli.commands.setup import setup
from src.cli.commands.start import start
from src.cli.commands.status import status
from src.cli.commands.stop import stop
from src.cli.commands.test import test
from src.cli.commands.validate import validate

console = Console()


@click.group()
@click.version_option(version="0.1.0", prog_name="cdc-demo")
@click.pass_context
def cli(ctx: click.Context) -> None:
    """
    CDC Demo - Change Data Capture demonstration for open-source data storages.

    This tool demonstrates CDC implementations for:
    - PostgreSQL (logical replication with Debezium)
    - MySQL (binlog replication with Debezium)
    - DeltaLake (Change Data Feed)
    - Apache Iceberg (snapshot-based incremental reads)
    """
    ctx.ensure_object(dict)


# Register command groups
cli.add_command(setup)
cli.add_command(start)
cli.add_command(stop)
cli.add_command(status)
cli.add_command(generate)
cli.add_command(validate)
cli.add_command(monitor)
cli.add_command(test)
cli.add_command(cleanup)


if __name__ == "__main__":
    cli()

"""Generate command for creating mock data."""

import click
from rich.console import Console
from rich.progress import track

from src.data_generators.generators import (
    CustomerGenerator,
    OrderGenerator,
    ProductGenerator,
)

console = Console()


@click.command()
@click.argument("entity", type=click.Choice(["customers", "orders", "products", "all"]))
@click.option("--count", "-c", default=100, help="Number of records to generate")
@click.option("--seed", type=int, help="Random seed for reproducibility")
def generate(entity: str, count: int, seed: int | None) -> None:
    """
    Generate mock data for CDC testing.

    ENTITY can be: customers, orders, products, or all
    """
    console.print(f"\n[bold blue]Generating {entity} data[/bold blue]\n")

    try:
        if entity == "customers" or entity == "all":
            _generate_customers(count, seed)

        if entity == "products" or entity == "all":
            _generate_products(count // 5, seed)

        if entity == "orders" or entity == "all":
            _generate_orders(count * 2, seed)

        console.print("\n[bold green]✓ Data generation complete![/bold green]\n")

    except Exception as e:
        console.print(f"[red]✗ Failed to generate data: {e}[/red]")
        raise click.Abort()


def _generate_customers(count: int, seed: int | None) -> None:
    """Generate customer records."""
    generator = CustomerGenerator(seed=seed)
    customers = []

    for _ in track(range(count), description="Generating customers..."):
        customers.extend(generator.generate(1))

    console.print(f"[green]✓ Generated {len(customers)} customers[/green]")
    # TODO: Insert into database


def _generate_products(count: int, seed: int | None) -> None:
    """Generate product records."""
    generator = ProductGenerator(seed=seed)
    products = []

    for _ in track(range(count), description="Generating products..."):
        products.extend(generator.generate(1))

    console.print(f"[green]✓ Generated {len(products)} products[/green]")
    # TODO: Insert into database


def _generate_orders(count: int, seed: int | None) -> None:
    """Generate order records."""
    # TODO: Fetch customer IDs from database
    customer_ids = list(range(1, 101))  # Placeholder
    generator = OrderGenerator(customer_ids=customer_ids, seed=seed)
    orders = []

    for _ in track(range(count), description="Generating orders..."):
        orders.extend(generator.generate(1))

    console.print(f"[green]✓ Generated {len(orders)} orders[/green]")
    # TODO: Insert into database

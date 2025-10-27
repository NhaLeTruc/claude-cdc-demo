"""Data generators for mock CDC data using Faker."""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional
import random

from faker import Faker

from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class DataGenerator(ABC):
    """Base class for data generators."""

    def __init__(self, locale: str = "en_US", seed: Optional[int] = None) -> None:
        """
        Initialize data generator.

        Args:
            locale: Faker locale
            seed: Random seed for reproducibility
        """
        self.faker = Faker(locale)
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

    @abstractmethod
    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """
        Generate mock data records.

        Args:
            count: Number of records to generate

        Returns:
            List of generated records
        """
        pass


class CustomerGenerator(DataGenerator):
    """Generator for customer data."""

    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate customer records."""
        customers = []
        for _ in range(count):
            customer = {
                "first_name": self.faker.first_name(),
                "last_name": self.faker.last_name(),
                "email": self.faker.unique.email(),
                "phone": self.faker.phone_number(),
                "address": self.faker.street_address(),
                "city": self.faker.city(),
                "state": self.faker.state_abbr(),
                "zip_code": self.faker.zipcode(),
                "country": "USA",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            customers.append(customer)

        logger.info(f"Generated {count} customer records")
        return customers


class ProductGenerator(DataGenerator):
    """Generator for product data."""

    CATEGORIES = [
        "Electronics",
        "Furniture",
        "Clothing",
        "Books",
        "Toys",
        "Sports",
        "Home & Garden",
        "Automotive",
    ]

    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate product records."""
        products = []
        for _ in range(count):
            product = {
                "product_name": self.faker.catch_phrase(),
                "description": self.faker.text(max_nb_chars=200),
                "category": random.choice(self.CATEGORIES),
                "price": round(Decimal(random.uniform(9.99, 999.99)), 2),
                "stock_quantity": random.randint(0, 500),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            products.append(product)

        logger.info(f"Generated {count} product records")
        return products


class OrderGenerator(DataGenerator):
    """Generator for order data."""

    STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]

    def __init__(
        self,
        customer_ids: List[int],
        locale: str = "en_US",
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize order generator.

        Args:
            customer_ids: List of valid customer IDs
            locale: Faker locale
            seed: Random seed
        """
        super().__init__(locale, seed)
        self.customer_ids = customer_ids

    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate order records."""
        if not self.customer_ids:
            raise ValueError("No customer IDs provided")

        orders = []
        for _ in range(count):
            order = {
                "customer_id": random.choice(self.customer_ids),
                "order_date": self.faker.date_time_between(
                    start_date="-30d", end_date="now"
                ),
                "total_amount": round(Decimal(random.uniform(10.0, 1000.0)), 2),
                "status": random.choice(self.STATUSES),
                "shipping_address": self.faker.address(),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            orders.append(order)

        logger.info(f"Generated {count} order records")
        return orders


class OrderItemGenerator(DataGenerator):
    """Generator for order item data."""

    def __init__(
        self,
        order_ids: List[int],
        product_ids: List[int],
        locale: str = "en_US",
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize order item generator.

        Args:
            order_ids: List of valid order IDs
            product_ids: List of valid product IDs
            locale: Faker locale
            seed: Random seed
        """
        super().__init__(locale, seed)
        self.order_ids = order_ids
        self.product_ids = product_ids

    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate order item records."""
        if not self.order_ids or not self.product_ids:
            raise ValueError("No order or product IDs provided")

        items = []
        for _ in range(count):
            quantity = random.randint(1, 5)
            unit_price = round(Decimal(random.uniform(9.99, 299.99)), 2)

            item = {
                "order_id": random.choice(self.order_ids),
                "product_id": random.choice(self.product_ids),
                "quantity": quantity,
                "unit_price": unit_price,
                "created_at": datetime.now(),
            }
            items.append(item)

        logger.info(f"Generated {count} order item records")
        return items


class InventoryTransactionGenerator(DataGenerator):
    """Generator for inventory transaction data."""

    TRANSACTION_TYPES = ["IN", "OUT", "ADJUSTMENT"]
    REASONS = [
        "Purchase order received",
        "Customer order fulfilled",
        "Inventory adjustment",
        "Damaged goods",
        "Return processed",
        "Stock transfer",
    ]

    def __init__(
        self,
        product_ids: List[int],
        locale: str = "en_US",
        seed: Optional[int] = None,
    ) -> None:
        """
        Initialize inventory transaction generator.

        Args:
            product_ids: List of valid product IDs
            locale: Faker locale
            seed: Random seed
        """
        super().__init__(locale, seed)
        self.product_ids = product_ids

    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate inventory transaction records."""
        if not self.product_ids:
            raise ValueError("No product IDs provided")

        transactions = []
        for _ in range(count):
            transaction_type = random.choice(self.TRANSACTION_TYPES)
            previous_qty = random.randint(50, 500)

            if transaction_type == "IN":
                quantity = random.randint(10, 100)
                new_qty = previous_qty + quantity
            elif transaction_type == "OUT":
                quantity = -random.randint(1, min(50, previous_qty))
                new_qty = previous_qty + quantity
            else:  # ADJUSTMENT
                quantity = random.randint(-20, 20)
                new_qty = max(0, previous_qty + quantity)

            transaction = {
                "product_id": random.choice(self.product_ids),
                "transaction_type": transaction_type,
                "quantity": abs(quantity),
                "previous_quantity": previous_qty,
                "new_quantity": new_qty,
                "reason": random.choice(self.REASONS),
                "transaction_date": self.faker.date_time_between(
                    start_date="-7d", end_date="now"
                ),
            }
            transactions.append(transaction)

        logger.info(f"Generated {count} inventory transaction records")
        return transactions


class SupplierGenerator(DataGenerator):
    """Generator for supplier data."""

    def generate(self, count: int = 1) -> List[Dict[str, Any]]:
        """Generate supplier records."""
        suppliers = []
        for _ in range(count):
            supplier = {
                "supplier_name": self.faker.company(),
                "contact_name": self.faker.name(),
                "email": self.faker.unique.company_email(),
                "phone": self.faker.phone_number(),
                "address": self.faker.address(),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            suppliers.append(supplier)

        logger.info(f"Generated {count} supplier records")
        return suppliers


class CDCEventGenerator:
    """Generator for CDC events (for testing)."""

    def __init__(self, seed: Optional[int] = None) -> None:
        """
        Initialize CDC event generator.

        Args:
            seed: Random seed
        """
        self.faker = Faker()
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

    def generate_insert_event(
        self, table: str, data: Dict[str, Any], source: str = "postgres"
    ) -> Dict[str, Any]:
        """
        Generate INSERT CDC event.

        Args:
            table: Table name
            data: Row data
            source: Source database

        Returns:
            CDC event
        """
        return {
            "event_id": self.faker.uuid4(),
            "operation": "INSERT",
            "source": source,
            "table": table,
            "timestamp": {
                "event_time": datetime.now().isoformat(),
                "processing_time": datetime.now().isoformat(),
            },
            "before": None,
            "after": data,
        }

    def generate_update_event(
        self,
        table: str,
        before_data: Dict[str, Any],
        after_data: Dict[str, Any],
        source: str = "postgres",
    ) -> Dict[str, Any]:
        """
        Generate UPDATE CDC event.

        Args:
            table: Table name
            before_data: Data before update
            after_data: Data after update
            source: Source database

        Returns:
            CDC event
        """
        return {
            "event_id": self.faker.uuid4(),
            "operation": "UPDATE",
            "source": source,
            "table": table,
            "timestamp": {
                "event_time": datetime.now().isoformat(),
                "processing_time": datetime.now().isoformat(),
            },
            "before": before_data,
            "after": after_data,
        }

    def generate_delete_event(
        self, table: str, data: Dict[str, Any], source: str = "postgres"
    ) -> Dict[str, Any]:
        """
        Generate DELETE CDC event.

        Args:
            table: Table name
            data: Deleted row data
            source: Source database

        Returns:
            CDC event
        """
        return {
            "event_id": self.faker.uuid4(),
            "operation": "DELETE",
            "source": source,
            "table": table,
            "timestamp": {
                "event_time": datetime.now().isoformat(),
                "processing_time": datetime.now().isoformat(),
            },
            "before": data,
            "after": None,
        }

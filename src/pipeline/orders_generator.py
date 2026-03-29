import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from faker import Faker
from loguru import logger

from src.pipeline.config import RAW_DATA_DIR

# Initialize Faker with French locale
# This generates realistic French names, addresses, etc.
fake = Faker("fr_FR")

# Fix the random seed for reproducibility
# Same seed = same data generated every time
# Important for testing and debugging
random.seed(42)
fake.seed_instance(42)

ORDERS_PER_YEAR = {
    2017: 15_000,
    2018: 22_000,
    2019: 30_000,
    2020: 65_000,
    2021: 58_000,
    2022: 45_000,
    2023: 52_000,
    2024: 48_000,
}

BASKET_BY_YEAR = {
    2017: 45,
    2018: 45,
    2019: 45,
    2020: 72,
    2021: 58,
    2022: 52,
    2023: 52,
    2024: 52,
}

ORDER_STATUSES = {
    "delivered": 0.78,
    "shipped": 0.08,
    "cancelled": 0.10,
    "pending": 0.04,
}

CUSTOMER_SEGMENTS = {
    "loyal": 0.20,
    "occasional": 0.50,
    "one_shot": 0.30,
}

SEASONAL_BOOST = {
    "chocolat": {12: 1.8, 11: 1.3},
    "champagne": {12: 2.2, 11: 1.5},
    "glace": {6: 1.9, 7: 2.0, 8: 1.8},
    "soupe": {11: 1.4, 12: 1.4, 1: 1.4, 2: 1.3},
    "bio": {1: 1.3},
}

DIRTY_RATES = {
    "duplicate": 0.005,
    "bad_date": 0.01,
    "bad_status": 0.02,
    "null_customer": 0.03,
    "bad_price": 0.005,
    "bad_fk": 0.01,
}

CHANNELS_BY_YEAR = {
    2017: {"mobile": 0.30, "desktop": 0.65, "app": 0.05},
    2018: {"mobile": 0.35, "desktop": 0.58, "app": 0.07},
    2019: {"mobile": 0.42, "desktop": 0.50, "app": 0.08},
    2020: {"mobile": 0.48, "desktop": 0.40, "app": 0.12},
    2021: {"mobile": 0.52, "desktop": 0.33, "app": 0.15},
    2022: {"mobile": 0.55, "desktop": 0.28, "app": 0.17},
    2023: {"mobile": 0.58, "desktop": 0.23, "app": 0.19},
    2024: {"mobile": 0.60, "desktop": 0.18, "app": 0.22},
}


class OrdersGenerator:
    def __init__(self, product_categories: dict[str, str]):
        """
        Args:
            product_categories: dict mapping product_id → primary_category
        """
        self.product_categories = product_categories
        self.product_ids = list(product_categories.keys())
        self.output_dir = RAW_DATA_DIR / "orders"

        total_customers = 10_000
        n_loyal = int(total_customers * 0.20)
        n_occasional = int(total_customers * 0.50)
        n_one_shot = int(total_customers * 0.30)

        self.loyal_customers = [str(uuid.uuid4()) for _ in range(n_loyal)]
        self.occasional_customers = [str(uuid.uuid4()) for _ in range(n_occasional)]
        self.one_shot_customers = [str(uuid.uuid4()) for _ in range(n_one_shot)]

        logger.info(
            f"OrdersGenerator initialized — "
            f"{len(self.product_ids)} products | "
            f"{total_customers} customers"
        )

    def _get_channel(self, year: int) -> str:
        """Pick a channel based on year distribution."""
        channels = CHANNELS_BY_YEAR[year]
        return random.choices(list(channels.keys()), weights=list(channels.values()))[0]

    def _get_status(self) -> str:
        """Pick an order status based on distribution."""
        return random.choices(
            list(ORDER_STATUSES.keys()), weights=list(ORDER_STATUSES.values())
        )[0]

    def _get_order_hour(self) -> int:
        """
        Generate a realistic order hour.
        Two peaks: morning (8-10) and evening (19-22).
        """
        peaks = (
            list(range(8, 11)) * 3  # morning peak
            + list(range(19, 23)) * 4  # evening peak
            + list(range(0, 24))  # base distribution
        )
        return random.choice(peaks)

    def _generate_order(self, order_id: str, year: int) -> dict:
        """
        Generate a single realistic order.
        Injects dirty data based on DIRTY_RATES probabilities.
        """
        # Pick customer based on segment distribution
        if random.random() < DIRTY_RATES["null_customer"]:
            customer_id = None
        else:
            segment = random.choices(
                list(CUSTOMER_SEGMENTS.keys()),
                weights=list(CUSTOMER_SEGMENTS.values())
            )[0]

            if segment == "loyal":
                customer_id = random.choice(self.loyal_customers)
            elif segment == "occasional":
                customer_id = random.choice(self.occasional_customers)
            else:
                customer_id = self.one_shot_customers.pop() if self.one_shot_customers else str(uuid.uuid4())

        # Status defined before date logic — always available
        status = self._get_status()

        # Generate order date
        if random.random() < DIRTY_RATES["bad_date"]:
            bad_formats = [
                f"{random.randint(1,28)}/{random.randint(1,12)}/{year}",
                f"{year}-{random.randint(1,12)}-{random.randint(32,45)}",
                f"{random.randint(1,12)}-{random.randint(1,28)}-{year}",
            ]
            order_date = random.choice(bad_formats)
            delivered_date = None
        else:
            order_date = fake.date_time_between(
                start_date=datetime(year, 1, 1),
                end_date=datetime(year, 12, 31),
            ).replace(hour=self._get_order_hour())

            if status == "delivered":
                delivered_date = (order_date + pd.Timedelta(days=random.randint(3, 7))).isoformat()
            else:
                delivered_date = None

            order_date = order_date.isoformat()

        # Inject bad status
        if random.random() < DIRTY_RATES["bad_status"]:
            bad_statuses = ["DELIVRD", "shiped", "Pending", "CANCEL", "deliver"]
            status = random.choice(bad_statuses)

        # Promo code — 15% of orders
        has_promo = random.random() < 0.15
        promo_code = fake.bothify(text="PROMO-??##") if has_promo else None
        discount = round(random.uniform(0.05, 0.20), 2) if has_promo else 0.0

        return {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date,
            "delivered_date": delivered_date,
            "status": status,
            "channel": self._get_channel(year),
            "has_promo": has_promo,
            "promo_code": promo_code,
            "discount": discount,
            "year": year,
        }

    def generate_orders(self) -> pd.DataFrame:
        """
        Generate all orders for all years based on ORDERS_PER_YEAR config.
        Injects duplicates based on DIRTY_RATES.
        """
        all_orders = []

        for year, n_orders in ORDERS_PER_YEAR.items():
            logger.info(f"Generating {n_orders} orders for {year}")

            for _ in range(n_orders):
                order_id = str(uuid.uuid4())
                order = self._generate_order(order_id, year)
                all_orders.append(order)

                # Inject duplicate orders
                if random.random() < DIRTY_RATES["duplicate"]:
                    all_orders.append(order)

            logger.success(f"{year} — {n_orders} orders generated")

        df = pd.DataFrame(all_orders)
        logger.info(f"Total orders generated : {len(df)}")
        return df

    def generate_order_items(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate order items for each order.
        Each order has 1 to 5 items linked to real product IDs.
        Applies seasonal weighting based on product category and month.
        Injects bad prices and invalid foreign keys based on DIRTY_RATES.
        """
        all_items = []

        for _, order in orders_df.iterrows():
            n_items = random.randint(1, 5)

            # Get order month for seasonal weighting
            try:
                order_month = datetime.fromisoformat(str(order["order_date"])).month
            except (ValueError, TypeError):
                order_month = None

            # Compute product weights based on seasonality
            if order_month:
                weights = []
                for pid in self.product_ids:
                    category = self.product_categories.get(pid, "").lower()
                    weight = 1.0
                    for season_keyword, boosts in SEASONAL_BOOST.items():
                        if season_keyword in category and order_month in boosts:
                            weight = boosts[order_month]
                            break
                    weights.append(weight)
            else:
                weights = None

            for _ in range(n_items):
                # Inject invalid foreign key
                if random.random() < DIRTY_RATES["bad_fk"]:
                    product_id = str(
                        random.randint(9_000_000_000_000, 9_999_999_999_999)
                    )
                else:
                    if weights:
                        product_id = random.choices(self.product_ids, weights=weights)[
                            0
                        ]
                    else:
                        product_id = random.choice(self.product_ids)

                # Inject aberrant price
                if random.random() < DIRTY_RATES["bad_price"]:
                    unit_price = random.choice([0.0, 999.99, -5.0])
                else:
                    unit_price = round(random.uniform(1.0, 30.0), 2)

                quantity = random.randint(1, 10)

                all_items.append(
                    {
                        "order_item_id": str(uuid.uuid4()),
                        "order_id": order["order_id"],
                        "product_id": product_id,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "total_price": round(quantity * unit_price, 2),
                    }
                )

        df = pd.DataFrame(all_items)
        logger.info(f"Total order items generated : {len(df)}")
        return df

    def to_parquet(
        self, orders_df: pd.DataFrame, order_items_df: pd.DataFrame
    ) -> tuple[Path, Path]:
        """
        Write orders and order_items to partitioned Parquet files.
        """
        now = datetime.now(timezone.utc)
        partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"

        # Orders
        orders_path = self.output_dir / "orders" / partition
        orders_path.mkdir(parents=True, exist_ok=True)
        orders_filepath = orders_path / "data.parquet"
        orders_df.to_parquet(orders_filepath, index=False)
        logger.success(f"{len(orders_df)} orders written → {orders_filepath}")

        # Order items
        order_items_path = self.output_dir / "order_items" / partition
        order_items_path.mkdir(parents=True, exist_ok=True)
        order_items_filepath = order_items_path / "data.parquet"
        order_items_df.to_parquet(order_items_filepath, index=False)
        logger.success(
            f"{len(order_items_df)} order items written → {order_items_filepath}"
        )

        return orders_filepath, order_items_filepath

    def run(self) -> tuple[Path, Path]:
        """
        Main entry point — orchestrates generation and storage.
        """
        logger.info("Starting orders generation")

        orders_df = self.generate_orders()
        order_items_df = self.generate_order_items(orders_df)
        orders_filepath, order_items_filepath = self.to_parquet(
            orders_df, order_items_df
        )

        logger.success(
            f"Generation complete — "
            f"{len(orders_df)} orders | {len(order_items_df)} order items"
        )

        return orders_filepath, order_items_filepath

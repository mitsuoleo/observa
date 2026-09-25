from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class PaymentSimulate(str, Enum):
    approve = "approve"
    reject = "reject"
    random = "random"


class StockSimulate(str, Enum):
    reserve = "reserve"
    unavailable = "unavailable"
    catalog = "catalog"


class SimulateFlags(BaseModel):
    payment: PaymentSimulate = PaymentSimulate.random
    stock: StockSimulate = StockSimulate.catalog


class OrderItemIn(BaseModel):
    product_id: UUID
    quantity: int = Field(ge=1)
    unit_price: Decimal = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_id: UUID
    items: list[OrderItemIn] = Field(min_length=1)
    simulate: SimulateFlags = Field(default_factory=SimulateFlags)

    @field_validator("items")
    @classmethod
    def unique_products(cls, items: list[OrderItemIn]) -> list[OrderItemIn]:
        if len({item.product_id for item in items}) != len(items):
            raise ValueError("duplicate product_id in items")
        return items


class OrderItemOut(BaseModel):
    product_id: UUID
    quantity: int
    unit_price: Decimal

    model_config = {"from_attributes": True}


class OrderOut(BaseModel):
    id: UUID
    customer_id: UUID
    status: Literal["PENDING", "CONFIRMED", "FAILED", "CANCELLED"]
    total_amount: Decimal
    items: list[OrderItemOut]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TimelineEntry(BaseModel):
    event_type: str
    payload: dict
    occurred_at: datetime

    model_config = {"from_attributes": True}

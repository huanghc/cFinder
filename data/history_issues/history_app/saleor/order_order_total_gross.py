from decimal import Decimal
from uuid import uuid4

from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import Q
from django.urls import reverse
from django.utils.timezone import now
from django.utils.translation import pgettext_lazy
from django_fsm import FSMField, transition
from django_prices.models import MoneyField, TaxedMoneyField
from payments import PaymentStatus, PurchasedItem
from payments.models import BasePayment
from prices import Money, TaxedMoney

from . import GroupStatus, OrderStatus
from ..account.models import Address
from ..core.utils import ZERO_TAXED_MONEY, build_absolute_uri
from ..discount.models import Voucher
from ..product.models import Product
from .transitions import (
    cancel_delivery_group, process_delivery_group, ship_delivery_group)


class OrderQuerySet(models.QuerySet):
    """Filters orders by status deduced from shipment groups."""

    def open(self):
        """Orders having at least one shipment group with status NEW."""
        return self.filter(Q(groups__status=GroupStatus.NEW))

    def closed(self):
        """Orders having no shipment groups with status NEW."""
        return self.filter(~Q(groups__status=GroupStatus.NEW))


class Order(models.Model):
    created = models.DateTimeField(
        default=now, editable=False)
    last_status_change = models.DateTimeField(
        default=now, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL, blank=True, null=True, related_name='orders',
        on_delete=models.SET_NULL)
    language_code = models.CharField(
        max_length=35, default=settings.LANGUAGE_CODE)
    tracking_client_id = models.CharField(
        max_length=36, blank=True, editable=False)
    billing_address = models.ForeignKey(
        Address, related_name='+', editable=False,
        on_delete=models.PROTECT)
    shipping_address = models.ForeignKey(
        Address, related_name='+', editable=False, null=True,
        on_delete=models.PROTECT)
    user_email = models.EmailField(
        blank=True, default='', editable=False)
    shipping_price_net = MoneyField(
        currency=settings.DEFAULT_CURRENCY, max_digits=12, decimal_places=2,
        default=0, editable=False)
    shipping_price_gross = MoneyField(
        currency=settings.DEFAULT_CURRENCY, max_digits=12, decimal_places=2,
        default=0, editable=False)
    shipping_price = TaxedMoneyField(
        net_field='shipping_price_net', gross_field='shipping_price_gross')
    token = models.CharField(max_length=36, unique=True)
    total_net = MoneyField(
        currency=settings.DEFAULT_CURRENCY, max_digits=12, decimal_places=2)
    total_gross = MoneyField(
        currency=settings.DEFAULT_CURRENCY, max_digits=12, decimal_places=2)
    total = TaxedMoneyField(net_field='total_net', gross_field='total_gross')
    voucher = models.ForeignKey(
        Voucher, null=True, related_name='+', on_delete=models.SET_NULL)
    discount_amount = MoneyField(
        currency=settings.DEFAULT_CURRENCY, max_digits=12, decimal_places=2,
        blank=True, null=True)
    discount_name = models.CharField(max_length=255, default='', blank=True)

    objects = OrderQuerySet.as_manager()

    class Meta:
        ordering = ('-last_status_change',)
        permissions = (
            ('view_order',
             pgettext_lazy('Permission description', 'Can view orders')),
            ('edit_order',
             pgettext_lazy('Permission description', 'Can edit orders')))

    def save(self, *args, **kwargs):
        if not self.token:
            self.token = str(uuid4())
        return super().save(*args, **kwargs)

    def get_lines(self):
        return OrderLine.objects.filter(delivery_group__order=self)

    def is_fully_paid(self):
        total_paid = sum(
            [
                payment.get_total_price() for payment in
                self.payments.filter(status=PaymentStatus.CONFIRMED)],
            TaxedMoney(
                net=Money(0, currency=settings.DEFAULT_CURRENCY),
                gross=Money(0, currency=settings.DEFAULT_CURRENCY)))
        return self.total.gross

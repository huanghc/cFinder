from decimal import Decimal as D
import warnings

from django.db import models
from django.conf import settings
from oscar.core.loading import get_model
from django.utils.translation import ugettext_lazy as _
from django.utils.importlib import import_module as django_import_module
from oscar.core.compat import AUTH_USER_MODEL

from oscar.core.loading import get_class
from oscar.models.fields import AutoSlugField
from oscar.apps.partner.exceptions import InvalidStockAdjustment
DefaultWrapper = get_class('partner.wrappers', 'DefaultWrapper')


# Cache dict of partner_id => availability wrapper instance
partner_wrappers = None

default_wrapper = DefaultWrapper()


def get_partner_wrapper(partner_id):
    """
    Returns the appropriate partner wrapper given the partner's PK
    """
    if partner_wrappers is None:
        _load_partner_wrappers()
    return partner_wrappers.get(partner_id, default_wrapper)


def _load_partner_wrappers():
    # Prime cache of partner wrapper dict
    global partner_wrappers
    partner_wrappers = {}
    Partner = get_model('partner', 'Partner')
    for code, class_str in settings.OSCAR_PARTNER_WRAPPERS.items():
        try:
            partner = Partner.objects.get(code=code)
        except Partner.DoesNotExist:
            continue
        else:
            module_path, klass = class_str.rsplit('.', 1)
            module = django_import_module(module_path)
            partner_wrappers[partner.id] = getattr(module, klass)()


class AbstractPartner(models.Model):
    """
    A fulfillment partner. An individual or company who can fulfil products.
    E.g. for physical goods, somebody with a warehouse and means of delivery.

    Creating one or more instances of the Partner model is a required step in
    setting up an Oscar deployment. Many Oscar deployments will only have one
    fulfillment partner.
    """
    code = AutoSlugField(_("Code"), max_length=128, unique=True,
                         populate_from='name')
    name = models.CharField(_("Name"), max_length=128, null=True, blank=True)

    #: A partner can have users assigned to it. This is used
    #: for access modelling in the permission-based dashboard
    users = models.ManyToManyField(
        AUTH_USER_MODEL, related_name="partners",
        blank=True, null=True, verbose_name=_("Users"))

    @property
    def display_name(self):
        if not self.name:
            return self.code
        return self.name

    @property
    def primary_address(self):
        """
        Returns a partners primary address. Usually that will be the
        headquarters or similar.

        This is a rudimentary implementation that raises an error if there's
        more than one address. If you actually want to support multiple
        addresses, you will likely need to extend PartnerAddress to have some
        field or flag to base your decision on.
        """
        addresses = self.addresses.all()
        if len(addresses) == 0:  # intentionally using len() to save queries
            return None
        elif len(addresses) == 1:
            return addresses[0]
        else:
            raise NotImplementedError(
                "Oscar's default implementation of primary_address only "
                "supports one PartnerAddress.  You need to override the "
                "primary_address to look up the right address")

    def get_address_for_stockrecord(self, stockrecord):
        """
        Stock might be coming from different warehouses. Overriding this
        function allows selecting the correct PartnerAddress for the record.
        That can be useful when determining tax.
        """
        return self.primary_address

    class Meta:
        permissions = (('dashboard_access', _('Can access dashboard')), )
        verbose_name = _('Fulfillment partner')
        verbose_name_plural = _('Fulfillment partners')
        abstract = True

    def __unicode__(self):
        return self.name


class AbstractStockRecord(models.Model):
    """
    A stock record.

    This records information about a product from a fulfilment partner, such as
    their SKU, the number they have in stock and price information.

    Stockrecords are used by 'strategies' to determine availability and pricing
    information for the customer.
    """
    product = models.ForeignKey(
        'catalogue.Product', related_name="stockrecords",
        verbose_name=_("Product"))
    partner = models.ForeignKey(
        'partner.Partner', verbose_name=_("Partner"),
        related_name='stockrecords')

    #: The fulfilment partner will often have their own SKU for a product,
    #: which we store here.  This will sometimes be the same the product's UPC
    #: but not always.  It should be unique per partner.
    #: See also http://en.wikipedia.org/wiki/Stock-keeping_unit
    partner_sku = models.CharField(_("Partner SKU"), max_length=128)

    # Price info:
    price_currency = models.CharField(
        _("Currency"), max_length=12, default=settings.OSCAR_DEFAULT_CURRENCY)

    # This is the base price for calculations - tax should be applied by the
    # appropriate method.  We don't store tax here as its calculation is highly
    # domain-specific.  It is NULLable because some items don't have a fixed
    # price but require a runtime calculation (possible from an external
    # service).
    price_excl_tax = models.DecimalField(
        _("Price (excl. tax)"), decimal_places=2, max_digits=12,
        blank=True, null=True)

    #: Retail price for this item.  This is simply the recommended price from
    #: the manufacturer.  If this is used, it is for display purposes only.
    #: This prices is the NOT the price charged to the customer.
    price_retail = models.DecimalField(
        _("Price (retail)"), decimal_places=2, max_digits=12,
        blank=True, null=True)

    #: Cost price is the price charged by the fulfilment partner.  It is not
    #: used (by default) in any price calculations but is often used in
    #: reporting so merchants can report on their profit margin.
    cost_price = models.DecimalField(
        _("Cost Price"), decimal_places=2, max_digits=12,
        blank=True, null=True)

    #: Number of items in stock
    num_in_stock = models.PositiveIntegerField(
        _("Number in stock"), blank=True, null=True)

    #: The amount of stock allocated to orders but not fed back to the master
    #: stock system.  A typical stock update process will set the num_in_stock
    #: variable to a new value and reset num_allocated to zero
    num_allocated = models.IntegerField(
        _("Number allocated"), blank=True, null=True)

    #: Threshold for low-stock alerts.  When stock goes beneath this threshold,
    #: an alert is triggered so warehouse managers can order more.
    low_stock_threshold = models.PositiveIntegerField(
        _("Low Stock Threshold"), blank=True, null=True)

    # Date information
    date_created = models.DateTimeField(_("Date created"), auto_now_add=True)
    date_updated = models.DateTimeField(_("Date updated"), auto_now=True,
                                        db_index=True)

    def __unicode__(self):
        msg = u"Partner: %s, product: %s" % (
            self.partner.display_name, self.product,)
        if self.partner_sku:
            msg = u"%s (%s)" % (msg, self.partner_sku)
        return msg

    class Meta:
        abstract = True
        unique_together = ('partner', 'partner_sku')
        verbose_name = _("Stock record")
        verbose_name_plural = _("Stock records")

    @property
    def net_stock_level(self):
        """
        The effective number in stock (eg available to buy).

        This is correct property to show the customer, not the num_in_stock
        field as that doesn't account for allocations.  This can be negative in
        some unusual circumstances
        """
        if self.num_in_stock is None:
            return 0
        if self.num_allocated is None:
            return self.num_in_stock
        return self.num_in_stock - self.num_allocated

    # 2-stage stock management model

    def allocate(self, quantity):
        """
        Record a stock allocation.

        This normally happens when a product is bought at checkout.  When the
        product is actually shipped, then we 'consume' the allocation.
        """
        if self.num_allocated is None:
            self.num_allocated = 0
        self.num_allocated += quantity
        self.save()
    allocate.alters_data = True

    def is_allocation_consumption_possible(self, quantity):
        """
        Test if a proposed stock consumption is permitted
        """
        return quantity <= min(self.num_allocated, self.num_in_stock)

    def consume_allocation(self, quantity):
        """
        Consume a previous allocation

        This is used when an item is shipped.  We remove the original
        allocation and adjust the number in stock accordingly
        """
        if not self.is_allocation_consumption_possible(quantity):
            raise InvalidStockAdjustment(
                _('Invalid stock consumption request'))
        self.num_allocated -= quantity
        self.num_in_stock -= quantity
        self.save()
    consume_allocation.alters_data = True

    def cancel_allocation(self, quantity):
        # We ignore requests that request a cancellation of more than the
        # amount already allocated.
        self.num_allocated -= min(self.num_allocated, quantity)
        self.save()
    cancel_allocation.alters_data = True

    @property
    def is_below_threshold(self):
        if self.low_stock_threshold is None:
            return False
        return self.net_stock_level < self.low_stock_threshold

    # Stock wrapper methods - deprecated since 0.6

    @property
    def is_available_to_buy(self):
        """
        Return whether this stockrecord allows the product to be purchased
        """
        warnings.warn((
            "StockRecord.is_available_to_buy is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine availability "
            "instead"), DeprecationWarning)
        return get_partner_wrapper(self.partner_id).is_available_to_buy(self)

    def is_purchase_permitted(self, user=None, quantity=1, product=None):
        """
        Return whether this stockrecord allows the product to be purchased by a
        specific user and quantity
        """
        warnings.warn((
            "StockRecord.is_purchase_permitted is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine availability "
            "instead"), DeprecationWarning)
        return get_partner_wrapper(
            self.partner_id).is_purchase_permitted(self, user, quantity,
                                                   product)

    @property
    def availability_code(self):
        """
        Return an product's availability as a code for use in CSS to add icons
        to the overall availability mark-up.  For example, "instock",
        "unavailable".
        """
        warnings.warn((
            "StockRecord.availability_code is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine availability "
            "instead"), DeprecationWarning)
        return get_partner_wrapper(self.partner_id).availability_code(self)

    @property
    def availability(self):
        """
        Return a product's availability as a string that can be displayed to
        the user.  For example, "In stock", "Unavailable".
        """
        warnings.warn((
            "StockRecord.availability is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine availability "
            "instead"), DeprecationWarning)
        return get_partner_wrapper(self.partner_id).availability(self)

    def max_purchase_quantity(self, user=None):
        """
        Return an item's availability as a string

        :param user: (optional) The user who wants to purchase
        """
        warnings.warn((
            "StockRecord.max_purchase_quantity is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine availability "
            "instead"), DeprecationWarning)
        return get_partner_wrapper(
            self.partner_id).max_purchase_quantity(self, user)

    @property
    def dispatch_date(self):
        """
        Return the estimated dispatch date for a line
        """
        warnings.warn((
            "StockRecord.dispatch_date is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine availability "
            "instead"), DeprecationWarning)
        return get_partner_wrapper(self.partner_id).dispatch_date(self)

    @property
    def lead_time(self):
        warnings.warn((
            "StockRecord.lead_time is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine availability "
            "instead"), DeprecationWarning)
        return get_partner_wrapper(self.partner_id).lead_time(self)

    # Price methods - deprecated in 0.6

    @property
    def price_incl_tax(self):
        """
        Return a product's price including tax.

        This defaults to the price_excl_tax as tax calculations are
        domain specific.  This class needs to be subclassed and tax logic
        added to this method.
        """
        warnings.warn((
            "StockRecord.price_incl_tax is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine price "
            "information instead"), DeprecationWarning, stacklevel=2)
        if self.price_excl_tax is None:
            return D('0.00')
        return self.price_excl_tax + self.price_tax

    @property
    def price_tax(self):
        """
        Return a product's tax value
        """
        warnings.warn((
            "StockRecord.price_incl_tax is deprecated and will be "
            "removed in 0.7.  Use a strategy class to determine price "
            "information instead"), DeprecationWarning)
        return get_partner_wrapper(self.partner_id).calculate_tax(self)


class AbstractStockAlert(models.Model):
    """
    A stock alert. E.g. used to notify users when a product is 'back in stock'.
    """
    stockrecord = models.ForeignKey(
        'partner.StockRecord', related_name='alerts',
        verbose_name=_("Stock Record"))
    threshold = models.PositiveIntegerField(_("Threshold"))
    OPEN, CLOSED = "Open", "Closed"
    status_choices = (
        (OPEN, _("Open")),
        (CLOSED, _("Closed")),
    )
    status = models.CharField(_("Status"), max_length=128, default=OPEN,
                              choices=status_choices)
    date_created = models.DateTimeField(_("Date Created"), auto_now_add=True)
    date_closed = models.DateTimeField(_("Date Closed"), blank=True, null=True)

    def close(self):
        self.status = self.CLOSED
        self.save()
    close.alters_data = True

    def __unicode__(self):
        return _('<stockalert for "%(stock)s" status %(status)s>') \
            % {'stock': self.stockrecord, 'status': self.status}

    class Meta:
        abstract = True
        ordering = ('-date_created',)
        verbose_name = _('Stock Alert')
        verbose_name_plural = _('Stock Alerts')


from django.contrib.sites.models import Site
from django.conf import settings
from oscar.core.loading import get_model
from django.utils.translation import ugettext_lazy as _

from oscar.apps.shipping.methods import Free
from oscar.core.loading import get_class
from . import exceptions

from decimal import Decimal as D

ShippingAddress = get_model('order', 'ShippingAddress')
Order = get_model('order', 'Order')
Line = get_model('order', 'Line')
LinePrice = get_model('order', 'LinePrice')
LineAttribute = get_model('order', 'LineAttribute')
OrderDiscount = get_model('order', 'OrderDiscount')
order_placed = get_class('order.signals', 'order_placed')


class OrderNumberGenerator(object):
    """
    Simple object for generating order numbers.

    We need this as the order number is often required for payment
    which takes place before the order model has been created.
    """

    def order_number(self, basket):
        """
        Return an order number for a given basket
        """
        return 100000 + basket.id


class OrderCreator(object):
    """
    Places the order by writing out the various models
    """

    def place_order(self, basket, total,  # noqa (too complex (12))
                    user=None, shipping_method=None, shipping_address=None,
                    billing_address=None, order_number=None, status=None,
                    **kwargs):
        """
        Placing an order involves creating all the relevant models based on the
        basket and session data.
        """
        if basket.is_empty:
            raise ValueError(_("Empty baskets cannot be submitted"))
        if not shipping_method:
            shipping_method = Free()
        if not order_number:
            generator = OrderNumberGenerator()
            order_number = generator.order_number(basket)
        if not status and hasattr(settings, 'OSCAR_INITIAL_ORDER_STATUS'):
            status = getattr(settings, 'OSCAR_INITIAL_ORDER_STATUS')
        try:
            Order._default_manager.get(number=order_number)
        except Order.DoesNotExist:
            pass
        else:
            raise ValueError(_("There is already an order with number %s")
                             % order_number)

        # Ok - everything seems to be in order, let's place the order
        order = self.create_order_model(
            user, basket, shipping_address, shipping_method, billing_address,
            total, order_number, status, **kwargs)
        for line in basket.all_lines():
            self.create_line_models(order, line)
            self.update_stock_records(line)

        for application in basket.offer_applications:
            # Trigger any deferred benefits from offers and capture the
            # resulting message
            application['message'] \
                = application['offer'].apply_deferred_benefit(basket)
            # Record offer application results
            if application['result'].affects_shipping:
                # Skip zero shipping discounts
                if shipping_method.discount <= D('0.00'):
                    continue
                # If a shipping offer, we need to grab the actual discount off
                # the shipping method instance, which should be wrapped in an
                # OfferDiscount instance.
                application['discount'] = shipping_method.discount
            self.create_discount_model(order, application)
            self.record_discount(application)

        for voucher in basket.vouchers.all():
            self.record_voucher_usage(order, voucher, user)

        # Send signal for analytics to pick up
        order_placed.send(sender=self, order=order, user=user)

        return order

    def create_order_model(self, user, basket, shipping_address,
                           shipping_method, billing_address, total,
                           order_number, status, **extra_order_fields):
        """
        Creates an order model.
        """
        order_data = {'basket': basket,
                      'number': order_number,
                      'site': Site._default_manager.get_current(),
                      'currency': total.currency,
                      'total_incl_tax': total.incl_tax,
                      'total_excl_tax': total.excl_tax,
                      'shipping_incl_tax': shipping_method.charge_incl_tax,
                      'shipping_excl_tax': shipping_method.charge_excl_tax,
                      'shipping_method': shipping_method.name,
                      'shipping_code': shipping_method.code}
        if shipping_address:
            order_data['shipping_address'] = shipping_address
        if billing_address:
            order_data['billing_address'] = billing_address
        if user and user.is_authenticated():
            order_data['user_id'] = user.id
        if status:
            order_data['status'] = status
        if extra_order_fields:
            order_data.update(extra_order_fields)
        order = Order(**order_data)
        order.save()
        return order

    def create_line_models(self, order, basket_line, extra_line_fields=None):
        """
        Create the batch line model.

        You can set extra fields by passing a dictionary as the
        extra_line_fields value
        """
        product = basket_line.product
        stockrecord = basket_line.stockrecord
        if not stockrecord:
            raise exceptions.UnableToPlaceOrder(
                "Baket line #%d has no stockrecord" % basket_line.id)
        partner = stockrecord.partner
        line_data = {
            'order': order,
            # Partner details
            'partner': partner,
            'partner_name': partner.name,
            'partner_sku': stockrecord.partner_sku,
            'stockrecord': stockrecord,
            # Product details
            'product': product,
            'title': product.get_title(),
            'upc': product.upc,
            'quantity': basket_line.quantity,
            # Price details
            'line_price_excl_tax':
            basket_line.line_price_excl_tax_incl_discounts,
            'line_price_incl_tax':
            basket_line.line_price_incl_tax_incl_discounts,
            'line_price_before_discounts_excl_tax':
            basket_line.line_price_excl_tax,
            'line_price_before_discounts_incl_tax':
            basket_line.line_price_incl_tax,
            # Reporting details
            'unit_cost_price': stockrecord.cost_price,
            'unit_price_incl_tax': basket_line.unit_price_incl_tax,
            'unit_price_excl_tax': basket_line.unit_price_excl_tax,
            'unit_retail_price': stockrecord.price_retail,
            # Shipping details
            'est_dispatch_date':
            basket_line.purchase_info.availability.dispatch_date
        }
        extra_line_fields = extra_line_fields or {}
        if hasattr(settings, 'OSCAR_INITIAL_LINE_STATUS'):
            if not (extra_line_fields and 'status' in extra_line_fields):
                extra_line_fields['status'] = getattr(
                    settings, 'OSCAR_INITIAL_LINE_STATUS')
        if extra_line_fields:
            line_data.update(extra_line_fields)

        order_line = Line._default_manager.create(**line_data)
        self.create_line_price_models(order, order_line, basket_line)
        self.create_line_attributes(order, order_line, basket_line)
        self.create_additional_line_models(order, order_line, basket_line)

        return order_line

    def update_stock_records(self, line):
        """
        Update any relevant stock records for this order line
        """
        if line.product.get_product_class().track_stock:
            line.stockrecord.allocate(line.quantity)

    def create_additional_line_models(self, order, order_line, basket_line):
        """
        Empty method designed to be overridden.

        Some applications require additional information about lines, this
        method provides a clean place to create additional models that
        relate to a given line.
        """
        pass

    def create_line_price_models(self, order, order_line, basket_line):
        """
        Creates the batch line price models
        """
        breakdown = basket_line.get_price_breakdown()
        for price_incl_tax, price_excl_tax, quantity in breakdown:
            order_line.prices.create(
                order=order,
                quantity=quantity,
                price_incl_tax=price_incl_tax,
                price_excl_tax=price_excl_tax)

    def create_line_attributes(self, order, order_line, basket_line):
        """
        Creates the batch line attributes.
        """
        for attr in basket_line.attributes.all():
            order_line.attributes.create(
                option=attr.option,
                type=attr.option.code,
                value=attr.value)

    def create_discount_model(self, order, discount):

        """
        Create an order discount model for each offer application attached to
        the basket.
        """
        order_discount = OrderDiscount(
            order=order,
            message=discount['message'],
            offer_id=discount['offer'].id,
            frequency=discount['freq'],
            amount=discount['discount'])
        result = discount['result']
        if result.affects_shipping:
            order_discount.category = OrderDiscount.SHIPPING
        elif result.affects_post_order:
            order_discount.category = OrderDiscount.DEFERRED
        voucher = discount.get('voucher', None)
        if voucher:
            order_discount.voucher_id = voucher.id
            order_discount.voucher_code = voucher.code
        order_discount.save()

    def record_discount(self, discount):
        discount['offer'].record_usage(discount)
        if 'voucher' in discount and discount['voucher']:
            discount['voucher'].record_discount(discount)

    def record_voucher_usage(self, order, voucher, user):
        """
        Updates the models that care about this voucher.
        """
        voucher.record_usage(order, user)

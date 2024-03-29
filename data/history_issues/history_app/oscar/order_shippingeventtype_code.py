import csv

from django.contrib import messages
from django.core.urlresolvers import reverse
from django.core.exceptions import ObjectDoesNotExist
from django.db.models.loading import get_model
from django.db.models import Sum, Count, fields, Q
from django.http import HttpResponse, HttpResponseRedirect, Http404
from django.shortcuts import get_object_or_404
from django.template.defaultfilters import date as format_date
from django.utils.datastructures import SortedDict
from django.views.generic import TemplateView, ListView, DetailView, UpdateView
from django.contrib import messages

from oscar.core.loading import get_class
from oscar.apps.dashboard.orders import forms
from oscar.apps.dashboard.views import BulkEditMixin

Order = get_model('order', 'Order')
OrderNote = get_model('order', 'OrderNote')
ShippingAddress = get_model('order', 'ShippingAddress')
Line = get_model('order', 'Line')
ShippingEventType = get_model('order', 'ShippingEventType')
EventHandler = get_class('order.processing', 'EventHandler')


class OrderSummaryView(TemplateView):
    template_name = 'dashboard/orders/summary.html'

    def get_context_data(self, **kwargs):
        status_breakdown = Order.objects.order_by('status').values('status').annotate(freq=Count('id'))

        return {'total_orders': Order.objects.all().count(),
                'total_lines': Line.objects.all().count(),
                'total_revenue': Order.objects.all().aggregate(Sum('total_incl_tax'))['total_incl_tax__sum'],
                'order_status_breakdown': status_breakdown, 
               }


class OrderListView(ListView, BulkEditMixin):
    model = Order
    context_object_name = 'orders'
    template_name = 'dashboard/orders/order_list.html'
    form_class = forms.OrderSearchForm
    base_description = 'All orders'
    paginate_by = 25
    description = ''
    actions = ('download_selected_orders',)
    current_view = 'dashboard:order-list'

    def get(self, request, *args, **kwargs):
        if 'order_number' in request.GET:
            try:
                order = Order.objects.get(number=request.GET['order_number'])
            except Order.DoesNotExist:
                pass
            else:
                return HttpResponseRedirect(reverse('dashboard:order-detail', kwargs={'number': order.number}))
        return super(OrderListView, self).get(request, *args, **kwargs)

    def get_queryset(self):
        """
        Build the queryset for this list and also update the title that 
        describes the queryset
        """
        queryset = self.model.objects.all().order_by('-date_placed')
        self.description = self.base_description

        # Look for shortcut query filters
        if 'order_status' in self.request.GET:
            self.form = self.form_class()
            status = self.request.GET['order_status']
            if status.lower() == 'none':
                self.description = "Orders without an order status"
                status = None
            else:
                self.description = "Orders with status '%s'" % status
            return self.model.objects.filter(status=status)

        if 'order_number' not in self.request.GET:
            self.form = self.form_class()
            return queryset

        self.form = self.form_class(self.request.GET)
        if not self.form.is_valid():
            return queryset

        data = self.form.cleaned_data

        if data['order_number']:
            queryset = self.model.objects.filter(number__istartswith=data['order_number'])
            self.description = 'Orders with number starting with "%s"' % data['order_number']

        if data['name']:
            # If the value is two words, then assume they are first name and last name
            parts = data['name'].split()
            if len(parts) == 2:
                queryset = queryset.filter(Q(user__first_name__istartswith=parts[0]) |
                                           Q(user__last_name__istartswith=parts[1])).distinct()
            else:
                queryset = queryset.filter(Q(user__first_name__istartswith=data['name']) |
                                           Q(user__last_name__istartswith=data['name'])).distinct()
            self.description += " with customer name matching '%s'" % data['name']

        if data['product_title']:
            queryset = queryset.filter(lines__title__istartswith=data['product_title']).distinct()
            self.description += " including an item with title matching '%s'" % data['product_title']

        if data['product_id']:
            queryset = queryset.filter(Q(lines__upc=data['product_id']) |
                                       Q(lines__product_id=data['product_id'])).distinct()
            self.description += " including an item with ID '%s'" % data['product_id']

        if data['date_from'] and data['date_to']:
            # Add 24 hours to make search inclusive
            date_to = data['date_to'] + datetime.timedelta(days=1)
            queryset = queryset.filter(date_placed__gte=data['date_from']).filter(date_placed__lt=date_to)
            self.description += " placed between %s and %s" % (format_date(data['date_from']), format_date(data['date_to']))
        elif data['date_from']:
            queryset = queryset.filter(date_placed__gte=data['date_from'])
            self.description += " placed since %s" % format_date(data['date_from'])
        elif data['date_to']:
            date_to = data['date_to'] + datetime.timedelta(days=1)
            queryset = queryset.filter(date_placed__lt=date_to)
            self.description += " placed before %s" % format_date(data['date_to'])

        if data['voucher']:
            queryset = queryset.filter(discounts__voucher_code=data['voucher']).distinct()
            self.description += " using voucher '%s'" % data['voucher']

        if data['payment_method']:
            queryset = queryset.filter(sources__source_type__code=data['payment_method']).distinct()
            self.description += " paid for by %s" % data['payment_method']

        if data['status']:
            queryset = queryset.filter(status=data['status'])
            self.description += " with status %s" % data['status']

        return queryset

    def get_context_data(self, **kwargs):
        ctx = super(OrderListView, self).get_context_data(**kwargs)
        ctx['queryset_description'] = self.description
        ctx['form'] = self.form
        return ctx

    def is_csv_download(self):
        return self.request.GET.get('response_format', None) == 'csv'

    def get_paginate_by(self, queryset):
        return None if self.is_csv_download() else self.paginate_by

    def render_to_response(self, context):
        if self.is_csv_download():
            return self.download_selected_orders(self.request, context['object_list'])
        return super(OrderListView, self).render_to_response(context)

    def download_selected_orders(self, request, orders):
        response = HttpResponse(mimetype='text/csv')
        response['Content-Disposition'] = 'attachment; filename=orders.csv'
        writer = csv.writer(response, delimiter=',')

        meta_data = (('number', 'Order number'),
                     ('value', 'Order value'),
                     ('date', 'Date of purchase'),
                     ('num_items', 'Number of items'),
                     ('status', 'Order status'),
                     ('shipping_address_name', 'Deliver to name'),
                     ('billing_address_name', 'Bill to name'),
                     )
        columns = SortedDict()
        for k,v in meta_data:
            columns[k] = v

        writer.writerow(columns.values())
        for order in orders:
            row = columns.copy()
            row['number'] = order.number
            row['value'] = order.total_incl_tax
            row['date'] = order.date_placed
            row['num_items'] = order.num_items
            row['status'] = order.status
            if order.shipping_address:
                row['shipping_address_name'] = order.shipping_address.name()
            else:
                row['shipping_address_name'] = ''
            if order.billing_address:
                row['billing_address_name'] = order.billing_address.name()
            else:
                row['billing_address_name'] = ''

            encoded_values = [unicode(value).encode('utf8') for value in row.values()]
            writer.writerow(encoded_values)
        return response


class OrderDetailView(DetailView):
    model = Order
    context_object_name = 'order'
    template_name = 'dashboard/orders/order_detail.html'
    order_actions = ('save_note', 'delete_note', 'change_order_status',)
    line_actions = ('change_line_statuses', 'create_shipping_event')

    def get_object(self):
        return get_object_or_404(self.model, number=self.kwargs['number'])
    
    def get_context_data(self, **kwargs):
        ctx = super(OrderDetailView, self).get_context_data(**kwargs)
        ctx['note_form'] = self.get_order_note_form()
        ctx['line_statuses'] = Line.all_statuses()
        ctx['shipping_event_types'] = ShippingEventType.objects.all()
        return ctx

    def get_order_note_form(self):
        post_data = None
        kwargs = {}
        if self.request.method == 'POST':
            post_data = self.request.POST
        note_id = self.kwargs.get('note_id', None)
        if note_id:
            note = get_object_or_404(OrderNote, order=self.object, id=note_id)
            kwargs['instance'] = note
        return forms.OrderNoteForm(post_data, **kwargs)

    def post(self, request, *args, **kwargs):
        self.object = self.get_object()
        order = self.object

        # Look for order-level action
        order_action = request.POST.get('order_action', '').lower()
        if order_action:
            if order_action not in self.order_actions:
                messages.error(self.request, "Invalid action")
                return self.reload_page_response()
            else:
                return getattr(self, order_action)(request, order)

        # Look for line-level action
        line_action = request.POST.get('line_action', '').lower()
        if line_action:
            if line_action not in self.line_actions:
                messages.error(self.request, "Invalid action")
                return self.reload_page_response()
            else:
                line_ids = request.POST.getlist('selected_line')
                line_quantities = request.POST.getlist('selected_line_qty')
                lines = order.lines.filter(id__in=line_ids)
                if lines.count() == 0:
                    messages.error(self.request, "You must select some lines to act on")
                    return self.reload_page_response()
                return getattr(self, line_action)(request, order, lines, line_quantities)

        messages.error(request, "No valid action submitted")
        return self.reload_page_response()

    def reload_page_response(self):
        return HttpResponseRedirect(reverse('dashboard:order-detail', kwargs={'number': self.object.number}))

    def save_note(self, request, order):
        form = self.get_order_note_form()
        success_msg = "Note saved"
        if form.is_valid():
            note = form.save(commit=False)
            note.user = request.user
            note.order = order
            note.save()
            messages.success(self.request, success_msg)
            return self.reload_page_response()
        ctx = self.get_context_data(note_form=form)
        return self.render_to_response(ctx)

    def delete_note(self, request, order):
        try:
            note = order.notes.get(id=request.POST.get('note_id', None))
        except ObjectDoesNotExist:
            messages.error(request, "Note cannot be deleted")
        else:
            messages.info(request, "Note deleted")
            note.delete()
        return self.reload_page_response()

    def change_order_status(self, request, order):
        new_status = request.POST['new_status'].strip()
        if not new_status:
            messages.error(request, "The new status '%s' is not valid" % new_status)
            return self.reload_page_response()
        if not new_status in order.available_statuses():
            messages.error(request, "The new status '%s' is not valid for this order" % new_status)
            return self.reload_page_response()

        handler = EventHandler()
        try:
            handler.handle_order_status_change(order, new_status)
        except PaymentError as e:
            messages.error("Unable to change order status due to payment error: %s" % e)
        else:
            msg = "Order status changed from '%s' to '%s'" % (order.status, new_status)
            messages.info(request, msg)
            order.notes.create(user=request.user, message=msg,
                            note_type=OrderNote.SYSTEM)
        return self.reload_page_response()

    def change_line_statuses(self, request, order, lines, quantities):
        new_status = request.POST['new_status'].strip()
        if not new_status:
            messages.error(request, "The new status '%s' is not valid" % new_status)
            return self.reload_page_response()
        errors = []
        for line in lines:
            if new_status not in line.available_statuses():
                errors.append("'%s' is not a valid new status for line %d" % (
                    new_status, line.id))
        if errors:
            messages.error(request, "\n".join(errors))
            return self.reload_page_response()

        msgs = []
        for line in lines:
            msg = "Status of line %d changed from '%s' to '%s'" % (
                line.id, line.status, new_status)
            msgs.append(msg)
            line.set_status(new_status)
        message = "\n".join(msgs)
        messages.info(request, message)
        order.notes.create(user=request.user, message=message,
                           note_type=OrderNote.SYSTEM)
        return self.reload_page_response()

    def create_shipping_event(self, request, order, lines, quantities):
        code = request.POST['shipping_event_type']
        try:
            event_type = ShippingEventType._default_manager.get(code=code)
        except ShippingEventType.DoesNotExist:
            messages.error(request, "The event type '%s' is not valid" % code)
            return self.reload_page_response()

        try:
            EventHandler().handle_order_status_change(order, new_status)
        except PaymentError as e:
            messages.error("Unable to change order status due to payment error: %s" % e)
        else:
            messages.info(request, "Shipping event created")
        return self.reload_page_response()


class LineDetailView(DetailView):
    model = Line
    context_object_name = 'line'
    template_name = 'dashboard/orders/line_detail.html'

    def get_object(self, queryset=None):
        try:
            return self.model.objects.get(pk=self.kwargs['line_id'])
        except self.model.DoesNotExist:
            raise Http404()

    def get_context_data(self, **kwargs):
        ctx = super(LineDetailView, self).get_context_data(**kwargs)
        ctx['order'] = self.object.order
        return ctx


def get_changes_between_models(model1, model2, excludes = []):
    changes = {}
    for field in model1._meta.fields:
        if not (isinstance(field, (fields.AutoField, fields.related.RelatedField))
                or field.name in excludes):
            if field.value_from_object(model1) != field.value_from_object(model2):
                changes[field.verbose_name] = (field.value_from_object(model1),
                                                   field.value_from_object(model2))
    return changes

def get_change_summary(model1, model2):
    """
    Generate a summary of the changes between two address models
    """
    changes = get_changes_between_models(model1, model2, ['search_text'])
    change_descriptions = []
    for field, delta in changes.items():
        change_descriptions.append(u"%s changed from '%s' to '%s'" % (field, delta[0], delta[1]))
    return "\n".join(change_descriptions)


class ShippingAddressUpdateView(UpdateView):
    model = ShippingAddress
    context_object_name = 'address'
    template_name = 'dashboard/orders/shippingaddress_form.html'
    form_class = forms.ShippingAddressForm

    def get_object(self):
        return get_object_or_404(self.model, order__number=self.kwargs['number'])

    def get_context_data(self, **kwargs):
        ctx = super(ShippingAddressUpdateView, self).get_context_data(**kwargs)
        ctx['order'] = self.object.order
        return ctx

    def form_valid(self, form):
        old_address = ShippingAddress.objects.get(id=self.object.id)
        response = super(ShippingAddressUpdateView, self).form_valid(form)
        changes = get_change_summary(old_address, self.object)
        if changes:
            msg = "Delivery address updated:\n%s" % changes
            self.object.order.notes.create(user=self.request.user, message=msg,
                                        note_type=OrderNote.SYSTEM)
        return response

    def get_success_url(self):
        messages.info(self.request, "Delivery address updated")
        return reverse('dashboard:order-detail', kwargs={'number': self.object.order.number,})

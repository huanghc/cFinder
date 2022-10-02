from django.contrib import messages
from django.contrib.sites.shortcuts import get_current_site
from django.shortcuts import get_object_or_404, redirect
from django.template.response import TemplateResponse
from django.utils.translation import ugettext_lazy as _

from .forms import AuthorizationKeyFormSet, SiteForm, SiteSettingForm
from ..views import superuser_required
from ...site.models import AuthorizationKey, SiteSettings
from ...site.utils import get_site_settings_from_request


from django.contrib.sites.models import Site


@superuser_required
def index(request):
    settings = get_site_settings_from_request(request)
    return redirect('dashboard:site-update', site_id=settings.pk)


@superuser_required
def update(request, site_id=None):
    site_settings = get_object_or_404(SiteSettings, pk=site_id)
    site = site_settings.site
    site_settings_form = SiteSettingForm(
        request.POST or None, instance=site_settings)
    site_form = SiteForm(request.POST or None, instance=site)
    authorization_qs = AuthorizationKey.objects.filter(
        site_settings=site_settings)
    formset = AuthorizationKeyFormSet(
        request.POST or None, queryset=authorization_qs,
        initial=[{'site_settings': site_settings}])
    if all([site_settings_form.is_valid(),
            formset.is_valid()]):
        site = site_form.save()
        site_settings.site = site
        site_settings = site_settings_form.save()
        formset.save()
        messages.success(request, _('Updated site %s') % site_settings)
        return redirect('dashboard:site-update', site_id=site_settings.id)
    ctx = {'site': site_settings, 'site_settings_form': site_settings_form,
           'site_form': site_form, 'formset': formset}
    return TemplateResponse(request, 'dashboard/sites/detail.html', ctx)

from __future__ import unicode_literals

from django.db import migrations

from django.contrib.sites.models import Site
from django.db import models

from django.utils.encoding import python_2_unicode_compatible
from django.utils.translation import pgettext_lazy

from . import AuthenticationBackends
from django.contrib.sites.models import _simple_domain_name_validator
from django.db import models
from django.utils.encoding import python_2_unicode_compatible
from django.utils.translation import pgettext_lazy

from . import AuthenticationBackends


@python_2_unicode_compatible
class SiteSettings(models.Model):
    domain = models.CharField(
        pgettext_lazy('Site field', 'domain'), max_length=100,
        validators=[_simple_domain_name_validator], unique=True)

    name = models.CharField(pgettext_lazy('Site field', 'name'), max_length=50)
    header_text = models.CharField(
        pgettext_lazy('Site field', 'header text'), max_length=200, blank=True)
    description = models.CharField(
        pgettext_lazy('Site field', 'site description'), max_length=500,
        blank=True)

    def __str__(self):
        return self.name

    def available_backends(self):
        return self.authorizationkey_set.values_list('name', flat=True)


@python_2_unicode_compatible
class AuthorizationKey(models.Model):
    site_settings = models.ForeignKey(SiteSettings)
    name = models.CharField(
        pgettext_lazy('Authentiaction field', 'name'), max_length=20,
        choices=AuthenticationBackends.BACKENDS)
    key = models.TextField(pgettext_lazy('Authentication field', 'key'))
    password = models.TextField(
        pgettext_lazy('Authentication field', 'password'))

    class Meta:
        unique_together = (('site_settings', 'name'),)

    def __str__(self):
        return self.name

    def key_and_secret(self):
        return self.key, self.password
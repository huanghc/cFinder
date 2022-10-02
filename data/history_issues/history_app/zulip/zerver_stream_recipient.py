from datetime import timedelta
from typing import Any, Dict, List, Mapping, Optional, Type
import mock

from django.core.management.base import BaseCommand
from django.utils.timezone import now as timezone_now

from analytics.lib.counts import COUNT_STATS, \
    CountStat, do_drop_all_analytics_tables
from analytics.lib.fixtures import generate_time_series_data
from analytics.lib.time_utils import time_range
from analytics.models import BaseCount, FillState, RealmCount, UserCount, \
    StreamCount, InstallationCount
from zerver.lib.actions import do_change_is_admin, STREAM_ASSIGNMENT_COLORS
from zerver.lib.create_user import create_user
from zerver.lib.timestamp import floor_to_day
from zerver.models import Realm, Stream, Client, \
    Recipient, Subscription

class Command(BaseCommand):

    def handle(self, *args: Any, **options: Any) -> None:
        # TODO: This should arguably only delete the objects
        # associated with the "analytics" realm.
        do_drop_all_analytics_tables()

        # This also deletes any objects with this realm as a foreign key
        Realm.objects.filter(string_id='analytics').delete()

        # Because we just deleted a bunch of objects in the database
        # directly (rather than deleting individual objects in Django,
        # in which case our post_save hooks would have flushed the
        # individual objects from memcached for us), we need to flush
        # memcached in order to ensure deleted objects aren't still
        # present in the memcached cache.
        from zerver.apps import flush_cache
        flush_cache(None)

        realm = Realm.objects.create(
            string_id='analytics', name='Analytics', date_created=installation_time)
        with mock.patch("zerver.lib.create_user.timezone_now", return_value=installation_time):
            shylock = create_user('shylock@analytics.ds', 'Shylock', realm,
                                  full_name='Shylock', short_name='shylock',
                                  is_realm_admin=True)
        do_change_is_admin(shylock, True)
        stream = Stream.objects.create(
            name='all', realm=realm, date_created=installation_time)
        recipient = Recipient.objects.create(type_id=stream.id, type=Recipient.STREAM)
        stream.recipient = recipient
        stream.save(update_fields=["recipient"])

        # Subscribe shylock to the stream to avoid invariant failures.
        # TODO: This should use subscribe_users_to_streams from populate_db.
        subs = [
            Subscription(recipient=recipient,
                         user_profile=shylock,
                         color=STREAM_ASSIGNMENT_COLORS[0]),
        ]
        Subscription.objects.bulk_create(subs)

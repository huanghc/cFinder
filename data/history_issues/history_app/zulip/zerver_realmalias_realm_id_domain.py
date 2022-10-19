from __future__ import absolute_import
from __future__ import print_function
from typing import (
    AbstractSet, Any, AnyStr, Callable, Dict, Iterable, Mapping, MutableMapping,
    Optional, Sequence, Set, Text, Tuple, TypeVar, Union
)

from django.utils.html import escape
from django.utils.translation import ugettext as _
from django.conf import settings
from django.core import validators
from django.contrib.sessions.models import Session
from zerver.lib.bugdown import (
    BugdownRenderingException,
    version as bugdown_version
)
from zerver.lib.cache import (
    to_dict_cache_key,
    to_dict_cache_key_id,
)
from zerver.lib.context_managers import lockfile
from zerver.lib.message import (
    access_message,
    MessageDict,
    message_to_dict,
    render_markdown,
)
from zerver.models import Realm, RealmEmoji, Stream, UserProfile, UserActivity, RealmAlias, \
    Subscription, Recipient, Message, Attachment, UserMessage, \
    Client, DefaultStream, UserPresence, Referral, PushDeviceToken, MAX_SUBJECT_LENGTH, \
    MAX_MESSAGE_LENGTH, get_client, get_stream, get_recipient, get_huddle, \
    get_user_profile_by_id, PreregistrationUser, get_display_recipient, \
    get_realm, bulk_get_recipients, \
    email_allowed_for_realm, email_to_username, display_recipient_cache_key, \
    get_user_profile_by_email, get_stream_cache_key, \
    UserActivityInterval, get_active_user_dicts_in_realm, get_active_streams, \
    realm_filters_for_realm, RealmFilter, receives_offline_notifications, \
    ScheduledJob, get_owned_bot_dicts, \
    get_old_unclaimed_attachments, get_cross_realm_emails, receives_online_notifications, \
    Reaction

from zerver.lib.alert_words import alert_words_in_realm
from zerver.lib.avatar import get_avatar_url, avatar_url

from django.db import transaction, IntegrityError, connection
from django.db.models import F, Q
from django.db.models.query import QuerySet
from django.core.exceptions import ValidationError
from importlib import import_module
from django.core.mail import EmailMessage
from django.utils.timezone import now

from confirmation.models import Confirmation
import six
from six.moves import filter
from six.moves import map
from six.moves import range
from six import unichr

session_engine = import_module(settings.SESSION_ENGINE)

from zerver.lib.create_user import random_api_key
from zerver.lib.timestamp import timestamp_to_datetime, datetime_to_timestamp
from zerver.lib.queue import queue_json_publish
from django.utils import timezone
from zerver.lib.create_user import create_user
from zerver.lib import bugdown
from zerver.lib.cache import cache_with_key, cache_set, \
    user_profile_by_email_cache_key, cache_set_many, \
    cache_delete, cache_delete_many
from zerver.decorator import statsd_increment
from zerver.lib.utils import log_statsd_event, statsd
from zerver.lib.html_diff import highlight_html_differences
from zerver.lib.alert_words import user_alert_words, add_user_alert_words, \
    remove_user_alert_words, set_user_alert_words
from zerver.lib.push_notifications import num_push_devices_for_user, \
    send_apple_push_notification, send_android_push_notification
from zerver.lib.notifications import clear_followup_emails_queue
from zerver.lib.narrow import check_supported_events_narrow_filter
from zerver.lib.request import JsonableError
from zerver.lib.session_user import get_session_user
from zerver.lib.upload import attachment_url_re, attachment_url_to_path_id, \
    claim_attachment, delete_message_image
from zerver.lib.str_utils import NonBinaryStr, force_str
from zerver.tornado.event_queue import request_event_queue, get_user_events, send_event

import DNS
import ujson
import time
import traceback
import re
import datetime
import os
import platform
import logging
import itertools
from collections import defaultdict
import copy

# This will be used to type annotate parameters in a function if the function
# works on both str and unicode in python 2 but in python 3 it only works on str.
SizedTextIterable = Union[Sequence[Text], AbstractSet[Text]]

STREAM_ASSIGNMENT_COLORS = [
    "#76ce90", "#fae589", "#a6c7e5", "#e79ab5",
    "#bfd56f", "#f4ae55", "#b0a5fd", "#addfe5",
    "#f5ce6e", "#c2726a", "#94c849", "#bd86e5",
    "#ee7e4a", "#a6dcbf", "#95a5fd", "#53a063",
    "#9987e1", "#e4523d", "#c2c2c2", "#4f8de4",
    "#c6a8ad", "#e7cc4d", "#c8bebf", "#a47462"]

# Store an event in the log for re-importing messages
def log_event(event):
    # type: (MutableMapping[str, Any]) -> None
    if settings.EVENT_LOG_DIR is None:
        return

    if "timestamp" not in event:
        event["timestamp"] = time.time()

    if not os.path.exists(settings.EVENT_LOG_DIR):
        os.mkdir(settings.EVENT_LOG_DIR)

    template = os.path.join(settings.EVENT_LOG_DIR,
                            '%s.' + platform.node() +
                            datetime.datetime.now().strftime('.%Y-%m-%d'))

    with lockfile(template % ('lock',)):
        with open(template % ('events',), 'a') as log:
            log.write(force_str(ujson.dumps(event) + u'\n'))

def active_user_ids(realm):
    # type: (Realm) -> List[int]
    return [userdict['id'] for userdict in get_active_user_dicts_in_realm(realm)]

def can_access_stream_user_ids(stream):
    # type: (Stream) -> Set[int]

    # return user ids of users who can access the attributes of
    # a stream, such as its name/description
    if stream.is_public():
        return set(active_user_ids(stream.realm))
    else:
        return private_stream_user_ids(stream)

def private_stream_user_ids(stream):
    # type: (Stream) -> Set[int]

    # TODO: Find similar queries elsewhere and de-duplicate this code.
    subscriptions = Subscription.objects.filter(
        recipient__type=Recipient.STREAM,
        recipient__type_id=stream.id,
        active=True)

    return {sub['user_profile_id'] for sub in subscriptions.values('user_profile_id')}

def bot_owner_userids(user_profile):
    # type: (UserProfile) -> Sequence[int]
    is_private_bot = (
        user_profile.default_sending_stream and user_profile.default_sending_stream.invite_only or
        user_profile.default_events_register_stream and user_profile.default_events_register_stream.invite_only)
    if is_private_bot:
        return (user_profile.bot_owner_id,) # TODO: change this to list instead of tuple
    else:
        return active_user_ids(user_profile.realm)

def realm_user_count(realm):
    # type: (Realm) -> int
    return UserProfile.objects.filter(realm=realm, is_active=True, is_bot=False).count()

def get_topic_history_for_stream(user_profile, recipient):
    # type: (UserProfile, Recipient) -> List[Tuple[str, int]]

    # We tested the below query on some large prod datasets, and we never
    # saw more than 50ms to execute it, so we think that's acceptable,
    # but we will monitor it, and we may later optimize it further.
    query = '''
        SELECT topic, read, count(*)
        FROM (
            SELECT
                ("zerver_usermessage"."flags" & 1) as read,
                "zerver_message"."subject" as topic,
                "zerver_message"."id" as message_id
            FROM "zerver_usermessage"
            INNER JOIN "zerver_message" ON (
                "zerver_usermessage"."message_id" = "zerver_message"."id"
            ) WHERE (
                "zerver_usermessage"."user_profile_id" = %s AND
                "zerver_message"."recipient_id" = %s
            ) ORDER BY "zerver_usermessage"."message_id" DESC
        ) messages_for_stream
        GROUP BY topic, read
        ORDER BY max(message_id) desc
    '''
    cursor = connection.cursor()
    cursor.execute(query, [user_profile.id, recipient.id])
    rows = cursor.fetchall()
    cursor.close()

    topic_names = dict() # type: Dict[str, str]
    topic_counts = dict() # type: Dict[str, int]
    topics = []
    for row in rows:
        topic_name, read, count = row
        if topic_name.lower() not in topic_names:
            topic_names[topic_name.lower()] = topic_name
        topic_name = topic_names[topic_name.lower()]
        if topic_name not in topic_counts:
            topic_counts[topic_name] = 0
            topics.append(topic_name)
        if not read:
            topic_counts[topic_name] += count

    history = [(topic, topic_counts[topic]) for topic in topics]
    return history

def send_signup_message(sender, signups_stream, user_profile,
                        internal=False, realm=None):
    # type: (UserProfile, Text, UserProfile, bool, Optional[Realm]) -> None
    if internal:
        # When this is done using manage.py vs. the web interface
        internal_blurb = " **INTERNAL SIGNUP** "
    else:
        internal_blurb = " "

    user_count = realm_user_count(user_profile.realm)
    # Send notification to realm notifications stream if it exists
    # Don't send notification for the first user in a realm
    if user_profile.realm.notifications_stream is not None and user_count > 1:
        internal_send_message(
            user_profile.realm,
            sender,
            "stream",
            user_profile.realm.notifications_stream.name,
            "New users", "%s just signed up for Zulip. Say hello!" % (
                user_profile.full_name,)
        )

    # We also send a notification to the Zulip administrative realm
    admin_realm = get_user_profile_by_email(sender).realm
    internal_send_message(
        admin_realm,
        sender,
        "stream",
        signups_stream,
        user_profile.realm.domain,
        "%s <`%s`> just signed up for Zulip!%s(total: **%i**)" % (
            user_profile.full_name,
            user_profile.email,
            internal_blurb,
            user_count,
        )
    )

def notify_new_user(user_profile, internal=False):
    # type: (UserProfile, bool) -> None
    if settings.NEW_USER_BOT is not None:
        send_signup_message(settings.NEW_USER_BOT, "signups", user_profile, internal)
    statsd.gauge("users.signups.%s" % (user_profile.realm.domain.replace('.', '_')), 1, delta=True)

def add_new_user_history(user_profile, streams):
    # type: (UserProfile, Iterable[Stream]) -> None
    """Give you the last 100 messages on your public streams, so you have
    something to look at in your home view once you finish the
    tutorial."""
    one_week_ago = now() - datetime.timedelta(weeks=1)
    recipients = Recipient.objects.filter(type=Recipient.STREAM,
                                          type_id__in=[stream.id for stream in streams
                                                       if not stream.invite_only])
    recent_messages = Message.objects.filter(recipient_id__in=recipients,
                                             pub_date__gt=one_week_ago).order_by("-id")
    message_ids_to_use = list(reversed(recent_messages.values_list('id', flat=True)[0:100]))
    if len(message_ids_to_use) == 0:
        return

    # Handle the race condition where a message arrives between
    # bulk_add_subscriptions above and the Message query just above
    already_ids = set(UserMessage.objects.filter(message_id__in=message_ids_to_use,
                                                 user_profile=user_profile).values_list("message_id", flat=True))
    ums_to_create = [UserMessage(user_profile=user_profile, message_id=message_id,
                                 flags=UserMessage.flags.read)
                     for message_id in message_ids_to_use
                     if message_id not in already_ids]

    UserMessage.objects.bulk_create(ums_to_create)

# Does the processing for a new user account:
# * Subscribes to default/invitation streams
# * Fills in some recent historical messages
# * Notifies other users in realm and Zulip about the signup
# * Deactivates PreregistrationUser objects
# * subscribe the user to newsletter if newsletter_data is specified
def process_new_human_user(user_profile, prereg_user=None, newsletter_data=None):
    # type: (UserProfile, Optional[PreregistrationUser], Optional[Dict[str, str]]) -> None
    mit_beta_user = user_profile.realm.is_zephyr_mirror_realm
    try:
        streams = prereg_user.streams.all()
    except AttributeError:
        # This will catch both the case where prereg_user is None and where it
        # is a MitUser.
        streams = []

    # If the user's invitation didn't explicitly list some streams, we
    # add the default streams
    if len(streams) == 0:
        streams = get_default_subs(user_profile)
    bulk_add_subscriptions(streams, [user_profile])

    add_new_user_history(user_profile, streams)

    # mit_beta_users don't have a referred_by field
    if not mit_beta_user and prereg_user is not None and prereg_user.referred_by is not None \
            and settings.NOTIFICATION_BOT is not None:
        # This is a cross-realm private message.
        internal_send_message(
            user_profile.realm,
            settings.NOTIFICATION_BOT,
            "private",
            prereg_user.referred_by.email,
            user_profile.realm.domain,
            "%s <`%s`> accepted your invitation to join Zulip!" % (
                user_profile.full_name,
                user_profile.email,
            )
        )
    # Mark any other PreregistrationUsers that are STATUS_ACTIVE as
    # inactive so we can keep track of the PreregistrationUser we
    # actually used for analytics
    if prereg_user is not None:
        PreregistrationUser.objects.filter(email__iexact=user_profile.email).exclude(
            id=prereg_user.id).update(status=0)
    else:
        PreregistrationUser.objects.filter(email__iexact=user_profile.email).update(status=0)

    notify_new_user(user_profile)

    if newsletter_data is not None:
        # If the user was created automatically via the API, we may
        # not want to register them for the newsletter
        queue_json_publish(
            "signups",
            {
                'email_address': user_profile.email,
                'merge_fields': {
                    'NAME': user_profile.full_name,
                    'REALM_ID': user_profile.realm_id,
                    'OPTIN_IP': newsletter_data["IP"],
                    'OPTIN_TIME': datetime.datetime.isoformat(now().replace(microsecond=0)),
                },
            },
            lambda event: None)

def notify_created_user(user_profile):
    # type: (UserProfile) -> None
    event = dict(type="realm_user", op="add",
                 person=dict(email=user_profile.email,
                             user_id=user_profile.id,
                             is_admin=user_profile.is_realm_admin,
                             full_name=user_profile.full_name,
                             is_bot=user_profile.is_bot))
    send_event(event, active_user_ids(user_profile.realm))

def notify_created_bot(user_profile):
    # type: (UserProfile) -> None

    def stream_name(stream):
        # type: (Stream) -> Optional[Text]
        if not stream:
            return None
        return stream.name

    default_sending_stream_name = stream_name(user_profile.default_sending_stream)
    default_events_register_stream_name = stream_name(user_profile.default_events_register_stream)

    event = dict(type="realm_bot", op="add",
                 bot=dict(email=user_profile.email,
                          user_id=user_profile.id,
                          full_name=user_profile.full_name,
                          api_key=user_profile.api_key,
                          default_sending_stream=default_sending_stream_name,
                          default_events_register_stream=default_events_register_stream_name,
                          default_all_public_streams=user_profile.default_all_public_streams,
                          avatar_url=avatar_url(user_profile),
                          owner=user_profile.bot_owner.email,
                          ))
    send_event(event, bot_owner_userids(user_profile))

def do_create_user(email, password, realm, full_name, short_name,
                   active=True, bot_type=None, bot_owner=None, tos_version=None,
                   avatar_source=UserProfile.AVATAR_FROM_GRAVATAR,
                   default_sending_stream=None, default_events_register_stream=None,
                   default_all_public_streams=None, prereg_user=None,
                   newsletter_data=None):
    # type: (Text, Text, Realm, Text, Text, bool, Optional[int], Optional[UserProfile], Optional[Text], Text, Optional[Stream], Optional[Stream], bool, Optional[PreregistrationUser], Optional[Dict[str, str]]) -> UserProfile
    event = {'type': 'user_created',
             'timestamp': time.time(),
             'full_name': full_name,
             'short_name': short_name,
             'user': email,
             'domain': realm.domain,
             'bot': bool(bot_type)}
    if bot_type:
        event['bot_owner'] = bot_owner.email
    log_event(event)

    user_profile = create_user(email=email, password=password, realm=realm,
                               full_name=full_name, short_name=short_name,
                               active=active, bot_type=bot_type, bot_owner=bot_owner,
                               tos_version=tos_version, avatar_source=avatar_source,
                               default_sending_stream=default_sending_stream,
                               default_events_register_stream=default_events_register_stream,
                               default_all_public_streams=default_all_public_streams)

    notify_created_user(user_profile)
    if bot_type:
        notify_created_bot(user_profile)
    else:
        process_new_human_user(user_profile, prereg_user=prereg_user,
                               newsletter_data=newsletter_data)
    return user_profile

def user_sessions(user_profile):
    # type: (UserProfile) -> List[Session]
    return [s for s in Session.objects.all()
            if get_session_user(s) == user_profile.id]

def delete_session(session):
    # type: (Session) -> None
    session_engine.SessionStore(session.session_key).delete() # type: ignore # import_module

def delete_user_sessions(user_profile):
    # type: (UserProfile) -> None
    for session in Session.objects.all():
        if get_session_user(session) == user_profile.id:
            delete_session(session)

def delete_realm_user_sessions(realm):
    # type: (Realm) -> None
    realm_user_ids = [user_profile.id for user_profile in
                      UserProfile.objects.filter(realm=realm)]
    for session in Session.objects.filter(expire_date__gte=datetime.datetime.now()):
        if get_session_user(session) in realm_user_ids:
            delete_session(session)

def delete_all_user_sessions():
    # type: () -> None
    for session in Session.objects.all():
        delete_session(session)

def delete_all_deactivated_user_sessions():
    # type: () -> None
    for session in Session.objects.all():
        user_profile_id = get_session_user(session)
        if user_profile_id is None:
            continue
        user_profile = get_user_profile_by_id(user_profile_id)
        if not user_profile.is_active or user_profile.realm.deactivated:
            logging.info("Deactivating session for deactivated user %s" % (user_profile.email,))
            delete_session(session)

def active_humans_in_realm(realm):
    # type: (Realm) -> Sequence[UserProfile]
    return UserProfile.objects.filter(realm=realm, is_active=True, is_bot=False)

def do_set_realm_name(realm, name):
    # type: (Realm, Text) -> None
    realm.name = name
    realm.save(update_fields=['name'])
    event = dict(
        type="realm",
        op="update",
        property='name',
        value=name,
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_restricted_to_domain(realm, restricted):
    # type: (Realm, bool) -> None
    realm.restricted_to_domain = restricted
    realm.save(update_fields=['restricted_to_domain'])
    event = dict(
        type="realm",
        op="update",
        property='restricted_to_domain',
        value=restricted,
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_invite_required(realm, invite_required):
    # type: (Realm, bool) -> None
    realm.invite_required = invite_required
    realm.save(update_fields=['invite_required'])
    event = dict(
        type="realm",
        op="update",
        property='invite_required',
        value=invite_required,
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_invite_by_admins_only(realm, invite_by_admins_only):
    # type: (Realm, bool) -> None
    realm.invite_by_admins_only = invite_by_admins_only
    realm.save(update_fields=['invite_by_admins_only'])
    event = dict(
        type="realm",
        op="update",
        property='invite_by_admins_only',
        value=invite_by_admins_only,
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_authentication_methods(realm, authentication_methods):
    # type: (Realm, Dict[str, bool]) -> None
    for key, value in list(authentication_methods.items()):
        index = getattr(realm.authentication_methods, key).number
        realm.authentication_methods.set_bit(index, int(value))
    realm.save(update_fields=['authentication_methods'])
    event = dict(
        type="realm",
        op="update_dict",
        property='default',
        data=dict(authentication_methods=realm.authentication_methods_dict())
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_create_stream_by_admins_only(realm, create_stream_by_admins_only):
    # type: (Realm, bool) -> None
    realm.create_stream_by_admins_only = create_stream_by_admins_only
    realm.save(update_fields=['create_stream_by_admins_only'])
    event = dict(
        type="realm",
        op="update",
        property='create_stream_by_admins_only',
        value=create_stream_by_admins_only,
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_add_emoji_by_admins_only(realm, add_emoji_by_admins_only):
    # type: (Realm, bool) -> None
    realm.add_emoji_by_admins_only = add_emoji_by_admins_only
    realm.save(update_fields=['add_emoji_by_admins_only'])
    event = dict(
        type="realm",
        op="update",
        property='add_emoji_by_admins_only',
        value=add_emoji_by_admins_only,
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_message_editing(realm, allow_message_editing, message_content_edit_limit_seconds):
    # type: (Realm, bool, int) -> None
    realm.allow_message_editing = allow_message_editing
    realm.message_content_edit_limit_seconds = message_content_edit_limit_seconds
    realm.save(update_fields=['allow_message_editing', 'message_content_edit_limit_seconds'])
    event = dict(
        type="realm",
        op="update_dict",
        property="default",
        data=dict(allow_message_editing=allow_message_editing,
                  message_content_edit_limit_seconds=message_content_edit_limit_seconds),
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_default_language(realm, default_language):
    # type: (Realm, Text) -> None

    realm.default_language = default_language
    realm.save(update_fields=['default_language'])
    event = dict(
        type="realm",
        op="update",
        property="default_language",
        value=default_language
    )
    send_event(event, active_user_ids(realm))

def do_set_realm_waiting_period_threshold(realm, threshold):
    # type: (Realm, int) -> None
    realm.waiting_period_threshold = threshold
    realm.save(update_fields=['waiting_period_threshold'])
    event = dict(
        type="realm",
        op="update",
        property='waiting_period_threshold',
        value=threshold,
    )
    send_event(event, active_user_ids(realm))

def do_deactivate_realm(realm):
    # type: (Realm) -> None
    """
    Deactivate this realm. Do NOT deactivate the users -- we need to be able to
    tell the difference between users that were intentionally deactivated,
    e.g. by a realm admin, and users who can't currently use Zulip because their
    realm has been deactivated.
    """
    if realm.deactivated:
        return

    realm.deactivated = True
    realm.save(update_fields=["deactivated"])

    for user in active_humans_in_realm(realm):
        # Don't deactivate the users, but do delete their sessions so they get
        # bumped to the login screen, where they'll get a realm deactivation
        # notice when they try to log in.
        delete_user_sessions(user)

def do_reactivate_realm(realm):
    # type: (Realm) -> None
    realm.deactivated = False
    realm.save(update_fields=["deactivated"])

def do_deactivate_user(user_profile, log=True, _cascade=True):
    # type: (UserProfile, bool, bool) -> None
    if not user_profile.is_active:
        return

    user_profile.is_active = False
    user_profile.save(update_fields=["is_active"])

    delete_user_sessions(user_profile)

    if log:
        log_event({'type': 'user_deactivated',
                   'timestamp': time.time(),
                   'user': user_profile.email,
                   'domain': user_profile.realm.domain})

    event = dict(type="realm_user", op="remove",
                 person=dict(email=user_profile.email,
                             user_id=user_profile.id,
                             full_name=user_profile.full_name))
    send_event(event, active_user_ids(user_profile.realm))

    if user_profile.is_bot:
        event = dict(type="realm_bot", op="remove",
                     bot=dict(email=user_profile.email,
                              user_id=user_profile.id,
                              full_name=user_profile.full_name))
        send_event(event, bot_owner_userids(user_profile))

    if _cascade:
        bot_profiles = UserProfile.objects.filter(is_bot=True, is_active=True,
                                                  bot_owner=user_profile)
        for profile in bot_profiles:
            do_deactivate_user(profile, _cascade=False)

def do_deactivate_stream(stream, log=True):
    # type: (Stream, bool) -> None
    user_profiles = UserProfile.objects.filter(realm=stream.realm)
    for user_profile in user_profiles:
        bulk_remove_subscriptions([user_profile], [stream])

    was_invite_only = stream.invite_only
    stream.deactivated = True
    stream.invite_only = True
    # Preserve as much as possible the original stream name while giving it a
    # special prefix that both indicates that the stream is deactivated and
    # frees up the original name for reuse.
    old_name = stream.name
    new_name = ("!DEACTIVATED:" + old_name)[:Stream.MAX_NAME_LENGTH]
    for i in range(20):
        existing_deactivated_stream = get_stream(new_name, stream.realm)
        if existing_deactivated_stream:
            # This stream has alrady been deactivated, keep prepending !s until
            # we have a unique stream name or you've hit a rename limit.
            new_name = ("!" + new_name)[:Stream.MAX_NAME_LENGTH]
        else:
            break

    # If you don't have a unique name at this point, this will fail later in the
    # code path.

    stream.name = new_name[:Stream.MAX_NAME_LENGTH]
    stream.save()

    # Remove the old stream information from remote cache.
    old_cache_key = get_stream_cache_key(old_name, stream.realm)
    cache_delete(old_cache_key)

    if not was_invite_only:
        stream_dict = stream.to_dict()
        stream_dict.update(dict(name=old_name, invite_only=was_invite_only))
        event = dict(type="stream", op="delete",
                     streams=[stream_dict])
        send_event(event, active_user_ids(stream.realm))

def do_change_user_email(user_profile, new_email):
    # type: (UserProfile, Text) -> None
    old_email = user_profile.email
    user_profile.email = new_email
    user_profile.save(update_fields=["email"])

    log_event({'type': 'user_email_changed',
               'old_email': old_email,
               'new_email': new_email})

def compute_irc_user_fullname(email):
    # type: (NonBinaryStr) -> NonBinaryStr
    return email.split("@")[0] + " (IRC)"

def compute_jabber_user_fullname(email):
    # type: (NonBinaryStr) -> NonBinaryStr
    return email.split("@")[0] + " (XMPP)"

def compute_mit_user_fullname(email):
    # type: (NonBinaryStr) -> NonBinaryStr
    try:
        # Input is either e.g. username@mit.edu or user|CROSSREALM.INVALID@mit.edu
        match_user = re.match(r'^([a-zA-Z0-9_.-]+)(\|.+)?@mit\.edu$', email.lower())
        if match_user and match_user.group(2) is None:
            answer = DNS.dnslookup(
                "%s.passwd.ns.athena.mit.edu" % (match_user.group(1),),
                DNS.Type.TXT)
            hesiod_name = force_str(answer[0][0]).split(':')[4].split(',')[0].strip()
            if hesiod_name != "":
                return hesiod_name
        elif match_user:
            return match_user.group(1).lower() + "@" + match_user.group(2).upper()[1:]
    except DNS.Base.ServerError:
        pass
    except:
        print("Error getting fullname for %s:" % (email,))
        traceback.print_exc()
    return email.lower()

@cache_with_key(lambda realm, email, f: user_profile_by_email_cache_key(email),
                timeout=3600*24*7)
def create_mirror_user_if_needed(realm, email, email_to_fullname):
    # type: (Realm, Text, Callable[[Text], Text]) -> UserProfile
    try:
        return get_user_profile_by_email(email)
    except UserProfile.DoesNotExist:
        try:
            # Forge a user for this person
            return create_user(email, None, realm,
                               email_to_fullname(email), email_to_username(email),
                               active=False, is_mirror_dummy=True)
        except IntegrityError:
            return get_user_profile_by_email(email)

def log_message(message):
    # type: (Message) -> None
    if not message.sending_client.name.startswith("test:"):
        log_event(message.to_log_dict())

def render_incoming_message(message, content, message_users, realm):
    # type: (Message, Text, Set[UserProfile], Realm) -> Text
    realm_alert_words = alert_words_in_realm(realm)
    try:
        rendered_content = render_markdown(
            message=message,
            content=content,
            realm=realm,
            realm_alert_words=realm_alert_words,
            message_users=message_users,
        )
    except BugdownRenderingException:
        raise JsonableError(_('Unable to render message'))
    return rendered_content

def get_recipient_user_profiles(recipient, sender_id):
    # type: (Recipient, Text) -> List[UserProfile]
    if recipient.type == Recipient.PERSONAL:
        recipients = list(set([get_user_profile_by_id(recipient.type_id),
                               get_user_profile_by_id(sender_id)]))
        # For personals, you send out either 1 or 2 copies, for
        # personals to yourself or to someone else, respectively.
        assert((len(recipients) == 1) or (len(recipients) == 2))
    elif (recipient.type == Recipient.STREAM or recipient.type == Recipient.HUDDLE):
        # We use select_related()/only() here, while the PERSONAL case above uses
        # get_user_profile_by_id() to get UserProfile objects from cache.  Streams will
        # typically have more recipients than PMs, so get_user_profile_by_id() would be
        # a bit more expensive here, given that we need to hit the DB anyway and only
        # care about the email from the user profile.
        fields = [
            'user_profile__id',
            'user_profile__email',
            'user_profile__enable_online_push_notifications',
            'user_profile__is_active',
            'user_profile__realm__domain'
        ]
        query = Subscription.objects.select_related("user_profile", "user_profile__realm").only(*fields).filter(
            recipient=recipient, active=True)
        recipients = [s.user_profile for s in query]
    else:
        raise ValueError('Bad recipient type')
    return recipients

def do_send_messages(messages):
    # type: (Sequence[Optional[MutableMapping[str, Any]]]) -> List[int]
    # Filter out messages which didn't pass internal_prep_message properly
    messages = [message for message in messages if message is not None]

    # Filter out zephyr mirror anomalies where the message was already sent
    already_sent_ids = [] # type: List[int]
    new_messages = [] # type: List[MutableMapping[str, Any]]
    for message in messages:
        if isinstance(message['message'], int):
            already_sent_ids.append(message['message'])
        else:
            new_messages.append(message)
    messages = new_messages

    # For consistency, changes to the default values for these gets should also be applied
    # to the default args in do_send_message
    for message in messages:
        message['rendered_content'] = message.get('rendered_content', None)
        message['no_log'] = message.get('no_log', False)
        message['stream'] = message.get('stream', None)
        message['local_id'] = message.get('local_id', None)
        message['sender_queue_id'] = message.get('sender_queue_id', None)
        message['realm'] = message.get('realm', message['message'].sender.realm)

    # Log the message to our message log for populate_db to refill
    for message in messages:
        if not message['no_log']:
            log_message(message['message'])

    for message in messages:
        message['recipients'] = get_recipient_user_profiles(message['message'].recipient,
                                                            message['message'].sender_id)
        # Only deliver the message to active user recipients
        message['active_recipients'] = [user_profile for user_profile in message['recipients']
                                        if user_profile.is_active]

    links_for_embed = set() # type: Set[Text]
    # Render our messages.
    for message in messages:
        assert message['message'].rendered_content is None
        rendered_content = render_incoming_message(
            message['message'],
            message['message'].content,
            message['active_recipients'],
            message['realm'])
        message['message'].rendered_content = rendered_content
        message['message'].rendered_content_version = bugdown_version
        links_for_embed |= message['message'].links_for_preview

    for message in messages:
        message['message'].update_calculated_fields()

    # Save the message receipts in the database
    user_message_flags = defaultdict(dict) # type: Dict[int, Dict[int, List[str]]]
    with transaction.atomic():
        Message.objects.bulk_create([message['message'] for message in messages])
        ums = [] # type: List[UserMessage]
        for message in messages:
            ums_to_create = [UserMessage(user_profile=user_profile, message=message['message'])
                             for user_profile in message['active_recipients']]

            # These properties on the Message are set via
            # render_markdown by code in the bugdown inline patterns
            wildcard = message['message'].mentions_wildcard
            mentioned_ids = message['message'].mentions_user_ids
            ids_with_alert_words = message['message'].user_ids_with_alert_words
            is_me_message = message['message'].is_me_message

            for um in ums_to_create:
                if um.user_profile.id == message['message'].sender.id and \
                        message['message'].sent_by_human():
                    um.flags |= UserMessage.flags.read
                if wildcard:
                    um.flags |= UserMessage.flags.wildcard_mentioned
                if um.user_profile_id in mentioned_ids:
                    um.flags |= UserMessage.flags.mentioned
                if um.user_profile_id in ids_with_alert_words:
                    um.flags |= UserMessage.flags.has_alert_word
                if is_me_message:
                    um.flags |= UserMessage.flags.is_me_message

                user_message_flags[message['message'].id][um.user_profile_id] = um.flags_list()
            ums.extend(ums_to_create)
        UserMessage.objects.bulk_create(ums)

        # Claim attachments in message
        for message in messages:
            if Message.content_has_attachment(message['message'].content):
                do_claim_attachments(message['message'])

    for message in messages:
        # Render Markdown etc. here and store (automatically) in
        # remote cache, so that the single-threaded Tornado server
        # doesn't have to.
        user_flags = user_message_flags.get(message['message'].id, {})
        sender = message['message'].sender
        user_presences = get_status_dict(sender)
        presences = {}
        for user_profile in message['active_recipients']:
            if user_profile.email in user_presences:
                presences[user_profile.id] = user_presences[user_profile.email]

        event = dict(
            type         = 'message',
            message      = message['message'].id,
            message_dict_markdown = message_to_dict(message['message'], apply_markdown=True),
            message_dict_no_markdown = message_to_dict(message['message'], apply_markdown=False),
            presences    = presences)
        users = [{'id': user.id,
                  'flags': user_flags.get(user.id, []),
                  'always_push_notify': user.enable_online_push_notifications}
                 for user in message['active_recipients']]
        if message['message'].recipient.type == Recipient.STREAM:
            # Note: This is where authorization for single-stream
            # get_updates happens! We only attach stream data to the
            # notify new_message request if it's a public stream,
            # ensuring that in the tornado server, non-public stream
            # messages are only associated to their subscribed users.
            if message['stream'] is None:
                message['stream'] = Stream.objects.select_related("realm").get(id=message['message'].recipient.type_id)
            if message['stream'].is_public():
                event['realm_id'] = message['stream'].realm_id
                event['stream_name'] = message['stream'].name
            if message['stream'].invite_only:
                event['invite_only'] = True
        if message['local_id'] is not None:
            event['local_id'] = message['local_id']
        if message['sender_queue_id'] is not None:
            event['sender_queue_id'] = message['sender_queue_id']
        send_event(event, users)

        if settings.INLINE_URL_EMBED_PREVIEW and links_for_embed:
            event_data = {
                'message_id': message['message'].id,
                'message_content': message['message'].content,
                'message_realm_id': message['realm'].id,
                'urls': links_for_embed}
            queue_json_publish('embed_links', event_data, lambda x: None)

        if (settings.ENABLE_FEEDBACK and
            message['message'].recipient.type == Recipient.PERSONAL and
                settings.FEEDBACK_BOT in [up.email for up in message['recipients']]):
            queue_json_publish(
                'feedback_messages',
                message_to_dict(message['message'], apply_markdown=False),
                lambda x: None
            )

    # Note that this does not preserve the order of message ids
    # returned.  In practice, this shouldn't matter, as we only
    # mirror single zephyr messages at a time and don't otherwise
    # intermingle sending zephyr messages with other messages.
    return already_sent_ids + [message['message'].id for message in messages]

def do_add_reaction(user_profile, message, emoji_name):
    # type: (UserProfile, Message, Text) -> None
    reaction = Reaction(user_profile=user_profile, message=message, emoji_name=emoji_name)
    reaction.save()

    user_dict = {'user_id': user_profile.id,
                 'email': user_profile.email,
                 'full_name': user_profile.full_name}

    event = {'type': 'reaction',
             'op': 'add',
             'user': user_dict,
             'message_id': message.id,
             'emoji_name': emoji_name} # type: Dict[str, Any]

    # Update the cached message since new reaction is added.
    update_to_dict_cache([message])

    # Recipients for message update events, including reactions, are
    # everyone who got the original message.  This means reactions
    # won't live-update in preview narrows, but it's the right
    # performance tradeoff, since otherwise we'd need to send all
    # reactions to public stream messages to every browser for every
    # client in the organization, which doesn't scale.
    ums = UserMessage.objects.filter(message=message.id)
    send_event(event, [um.user_profile_id for um in ums])

def do_remove_reaction(user_profile, message, emoji_name):
    # type: (UserProfile, Message, Text) -> None
    Reaction.objects.filter(user_profile=user_profile,
                            message=message,
                            emoji_name=emoji_name).delete()

    user_dict = {'user_id': user_profile.id,
                 'email': user_profile.email,
                 'full_name': user_profile.full_name} # type: Dict[str, Any]

    event = {'type': 'reaction',
             'op': 'remove',
             'user': user_dict,
             'message_id': message.id,
             'emoji_name': emoji_name} # type: Dict[str, Any]

    # Clear the cached message since reaction is removed.
    update_to_dict_cache([message])

    # Recipients for message update events, including reactions, are
    # everyone who got the original message.  This means reactions
    # won't live-update in preview narrows, but it's the right
    # performance tradeoff, since otherwise we'd need to send all
    # reactions to public stream messages to every browser for every
    # client in the organization, which doesn't scale.
    ums = UserMessage.objects.filter(message=message.id)
    send_event(event, [um.user_profile_id for um in ums])

def do_send_typing_notification(notification):
    # type: (Dict[str, Any]) -> None
    recipient_user_profiles = get_recipient_user_profiles(notification['recipient'],
                                                          notification['sender'].id)
    # Only deliver the notification to active user recipients
    user_ids_to_notify = [profile.id for profile in recipient_user_profiles if profile.is_active]
    sender_dict = {'user_id': notification['sender'].id, 'email': notification['sender'].email}
    # Include a list of recipients in the event body to help identify where the typing is happening
    recipient_dicts = [{'user_id': profile.id, 'email': profile.email} for profile in recipient_user_profiles]
    event = dict(
        type            = 'typing',
        op              = notification['op'],
        sender          = sender_dict,
        recipients      = recipient_dicts)

    send_event(event, user_ids_to_notify)

# check_send_typing_notification:
# Checks the typing notification and sends it
def check_send_typing_notification(sender, notification_to, operator):
    # type: (UserProfile, Sequence[Text], Text) -> None
    typing_notification = check_typing_notification(sender, notification_to, operator)
    do_send_typing_notification(typing_notification)

# check_typing_notification:
# Returns typing notification ready for sending with do_send_typing_notification on success
# or the error message (string) on error.
def check_typing_notification(sender, notification_to, operator):
    # type: (UserProfile, Sequence[Text], Text) -> Dict[str, Any]
    if len(notification_to) == 0:
        raise JsonableError(_('Missing parameter: \'to\' (recipient)'))
    elif operator not in ('start', 'stop'):
        raise JsonableError(_('Invalid \'op\' value (should be start or stop)'))
    else:
        try:
            recipient = recipient_for_emails(notification_to, False,
                                             sender, sender)
        except ValidationError as e:
            assert isinstance(e.messages[0], six.string_types)
            raise JsonableError(e.messages[0])
    if recipient.type == Recipient.STREAM:
        raise ValueError('Forbidden recipient type')
    return {'sender': sender, 'recipient': recipient, 'op': operator}

def create_stream_if_needed(realm, stream_name, invite_only=False, stream_description = ""):
    # type: (Realm, Text, bool, Text) -> Tuple[Stream, bool]
    (stream, created) = Stream.objects.get_or_create(
        realm=realm, name__iexact=stream_name,
        defaults={'name': stream_name,
                  'description': stream_description,
                  'invite_only': invite_only})
    if created:
        Recipient.objects.create(type_id=stream.id, type=Recipient.STREAM)
        if not invite_only:
            event = dict(type="stream", op="create",
                         streams=[stream.to_dict()])
            send_event(event, active_user_ids(realm))
    return stream, created

def create_streams_if_needed(realm, stream_dicts):
    # type: (Realm, List[Mapping[str, Any]]) -> Tuple[List[Stream], List[Stream]]
    """Note that stream_dict["name"] is assumed to already be stripped of
    whitespace"""
    added_streams = [] # type: List[Stream]
    existing_streams = [] # type: List[Stream]
    for stream_dict in stream_dicts:
        stream, created = create_stream_if_needed(realm,
                                                  stream_dict["name"],
                                                  invite_only=stream_dict.get("invite_only", False),
                                                  stream_description=stream_dict.get("description", ""))

        if created:
            added_streams.append(stream)
        else:
            existing_streams.append(stream)

    return added_streams, existing_streams

def recipient_for_emails(emails, not_forged_mirror_message,
                         user_profile, sender):
    # type: (Iterable[Text], bool, UserProfile, UserProfile) -> Recipient
    recipient_profile_ids = set()

    # We exempt cross-realm bots from the check that all the recipients
    # are in the same domain.
    realm_domains = set()
    exempt_emails = get_cross_realm_emails()
    if sender.email not in exempt_emails:
        realm_domains.add(sender.realm.domain)

    for email in emails:
        try:
            user_profile = get_user_profile_by_email(email)
        except UserProfile.DoesNotExist:
            raise ValidationError(_("Invalid email '%s'") % (email,))
        if (not user_profile.is_active and not user_profile.is_mirror_dummy) or \
                user_profile.realm.deactivated:
            raise ValidationError(_("'%s' is no longer using Zulip.") % (email,))
        recipient_profile_ids.add(user_profile.id)
        if email not in exempt_emails:
            realm_domains.add(user_profile.realm.domain)

    if not_forged_mirror_message and user_profile.id not in recipient_profile_ids:
        raise ValidationError(_("User not authorized for this query"))

    if len(realm_domains) > 1:
        raise ValidationError(_("You can't send private messages outside of your organization."))

    # If the private message is just between the sender and
    # another person, force it to be a personal internally
    if (len(recipient_profile_ids) == 2 and
            sender.id in recipient_profile_ids):
        recipient_profile_ids.remove(sender.id)

    if len(recipient_profile_ids) > 1:
        # Make sure the sender is included in huddle messages
        recipient_profile_ids.add(sender.id)
        huddle = get_huddle(list(recipient_profile_ids))
        return get_recipient(Recipient.HUDDLE, huddle.id)
    else:
        return get_recipient(Recipient.PERSONAL, list(recipient_profile_ids)[0])

def already_sent_mirrored_message_id(message):
    # type: (Message) -> Optional[int]
    if message.recipient.type == Recipient.HUDDLE:
        # For huddle messages, we use a 10-second window because the
        # timestamps aren't guaranteed to actually match between two
        # copies of the same message.
        time_window = datetime.timedelta(seconds=10)
    else:
        time_window = datetime.timedelta(seconds=0)

    messages = Message.objects.filter(
        sender=message.sender,
        recipient=message.recipient,
        content=message.content,
        subject=message.subject,
        sending_client=message.sending_client,
        pub_date__gte=message.pub_date - time_window,
        pub_date__lte=message.pub_date + time_window)

    if messages.exists():
        return messages[0].id
    return None

def extract_recipients(s):
    # type: (Union[str, Iterable[Text]]) -> List[Text]
    # We try to accept multiple incoming formats for recipients.
    # See test_extract_recipients() for examples of what we allow.
    try:
        data = ujson.loads(s) # type: ignore # This function has a super weird union argument.
    except ValueError:
        data = s

    if isinstance(data, six.string_types):
        data = data.split(',') # type: ignore # https://github.com/python/typeshed/pull/138

    if not isinstance(data, list):
        raise ValueError("Invalid data type for recipients")

    recipients = data

    # Strip recipients, and then remove any duplicates and any that
    # are the empty string after being stripped.
    recipients = [recipient.strip() for recipient in recipients]
    return list(set(recipient for recipient in recipients if recipient))

# check_send_message:
# Returns the id of the sent message.  Has same argspec as check_message.
def check_send_message(sender, client, message_type_name, message_to,
                       subject_name, message_content, realm=None, forged=False,
                       forged_timestamp=None, forwarder_user_profile=None, local_id=None,
                       sender_queue_id=None):
    # type: (UserProfile, Client, Text, Sequence[Text], Text, Text, Optional[Realm], bool, Optional[float], Optional[UserProfile], Optional[Text], Optional[Text]) -> int
    message = check_message(sender, client, message_type_name, message_to,
                            subject_name, message_content, realm, forged, forged_timestamp,
                            forwarder_user_profile, local_id, sender_queue_id)
    return do_send_messages([message])[0]

def check_stream_name(stream_name):
    # type: (Text) -> None
    if stream_name.strip() == "":
        raise JsonableError(_("Invalid stream name '%s'" % (stream_name)))
    if len(stream_name) > Stream.MAX_NAME_LENGTH:
        raise JsonableError(_("Stream name too long (limit: %s characters)" % (Stream.MAX_NAME_LENGTH)))

def send_pm_if_empty_stream(sender, stream, stream_name, realm):
    # type: (UserProfile, Stream, Text, Realm) -> None
    """If a bot sends a message to a stream that doesn't exist or has no
    subscribers, sends a notification to the bot owner (if not a
    cross-realm bot) so that the owner can correct the issue."""
    if sender.realm.is_zephyr_mirror_realm or sender.realm.deactivated:
        return

    if not sender.is_bot or sender.bot_owner is None:
        return

    # Don't send these notifications for cross-realm bot messages
    # (e.g. from EMAIL_GATEWAY_BOT) since the owner for
    # EMAIL_GATEWAY_BOT is probably the server administrator, not
    # the owner of the bot who could potentially fix the problem.
    if sender.realm != realm:
        return

    if stream is not None:
        num_subscribers = stream.num_subscribers()
        if num_subscribers > 0:
            return

    # We warn the user once every 5 minutes to avoid a flood of
    # PMs on a misconfigured integration, re-using the
    # UserProfile.last_reminder field, which is not used for bots.
    last_reminder = sender.last_reminder
    waitperiod = datetime.timedelta(minutes=UserProfile.BOT_OWNER_STREAM_ALERT_WAITPERIOD)
    if last_reminder and timezone.now() - last_reminder <= waitperiod:
        return

    if stream is None:
        error_msg = "that stream does not yet exist. To create it, "
    else:
        # num_subscribers == 0
        error_msg = "there are no subscribers to that stream. To join it, "

    content = ("Hi there! We thought you'd like to know that your bot **%s** just "
               "tried to send a message to stream `%s`, but %s"
               "click the gear in the left-side stream list." %
               (sender.full_name, stream_name, error_msg))
    message = internal_prep_message(realm, settings.NOTIFICATION_BOT, "private",
                                    sender.bot_owner.email, "", content)
    do_send_messages([message])

    sender.last_reminder = timezone.now()
    sender.save(update_fields=['last_reminder'])

# check_message:
# Returns message ready for sending with do_send_message on success or the error message (string) on error.
def check_message(sender, client, message_type_name, message_to,
                  subject_name, message_content, realm=None, forged=False,
                  forged_timestamp=None, forwarder_user_profile=None, local_id=None,
                  sender_queue_id=None):
    # type: (UserProfile, Client, Text, Sequence[Text], Text, Text, Optional[Realm], bool, Optional[float], Optional[UserProfile], Optional[Text], Optional[Text]) -> Dict[str, Any]
    stream = None
    if not message_to and message_type_name == 'stream' and sender.default_sending_stream:
        # Use the users default stream
        message_to = [sender.default_sending_stream.name]
    elif len(message_to) == 0:
        raise JsonableError(_("Message must have recipients"))
    if len(message_content.strip()) == 0:
        raise JsonableError(_("Message must not be empty"))
    message_content = truncate_body(message_content)

    if realm is None:
        realm = sender.realm

    if message_type_name == 'stream':
        if len(message_to) > 1:
            raise JsonableError(_("Cannot send to multiple streams"))

        stream_name = message_to[0].strip()
        check_stream_name(stream_name)

        if subject_name is None:
            raise JsonableError(_("Missing topic"))
        subject = subject_name.strip()
        if subject == "":
            raise JsonableError(_("Topic can't be empty"))
        subject = truncate_topic(subject)

        stream = get_stream(stream_name, realm)

        send_pm_if_empty_stream(sender, stream, stream_name, realm)

        if stream is None:
            raise JsonableError(_("Stream '%(stream_name)s' does not exist") % {'stream_name': escape(stream_name)})
        recipient = get_recipient(Recipient.STREAM, stream.id)

        if not stream.invite_only:
            # This is a public stream
            pass
        elif subscribed_to_stream(sender, stream):
            # Or it is private, but your are subscribed
            pass
        elif sender.is_api_super_user or (forwarder_user_profile is not None and
                                          forwarder_user_profile.is_api_super_user):
            # Or this request is being done on behalf of a super user
            pass
        elif sender.is_bot and subscribed_to_stream(sender.bot_owner, stream):
            # Or you're a bot and your owner is subscribed.
            pass
        else:
            # All other cases are an error.
            raise JsonableError(_("Not authorized to send to stream '%s'") % (stream.name,))

    elif message_type_name == 'private':
        mirror_message = client and client.name in ["zephyr_mirror", "irc_mirror", "jabber_mirror", "JabberMirror"]
        not_forged_mirror_message = mirror_message and not forged
        try:
            recipient = recipient_for_emails(message_to, not_forged_mirror_message,
                                             forwarder_user_profile, sender)
        except ValidationError as e:
            assert isinstance(e.messages[0], six.string_types)
            raise JsonableError(e.messages[0])
    else:
        raise JsonableError(_("Invalid message type"))

    message = Message()
    message.sender = sender
    message.content = message_content
    message.recipient = recipient
    if message_type_name == 'stream':
        message.subject = subject
    if forged and forged_timestamp is not None:
        # Forged messages come with a timestamp
        message.pub_date = timestamp_to_datetime(forged_timestamp)
    else:
        message.pub_date = timezone.now()
    message.sending_client = client

    # We render messages later in the process.
    assert message.rendered_content is None

    if client.name == "zephyr_mirror":
        id = already_sent_mirrored_message_id(message)
        if id is not None:
            return {'message': id}

    return {'message': message, 'stream': stream, 'local_id': local_id,
            'sender_queue_id': sender_queue_id, 'realm': realm}

def internal_prep_message(realm, sender_email, recipient_type_name, recipients,
                          subject, content):
    # type: (Realm, Text, str, Text, Text, Text) -> Optional[Dict[str, Any]]
    """
    Create a message object and checks it, but doesn't send it or save it to the database.
    The internal function that calls this can therefore batch send a bunch of created
    messages together as one database query.
    Call do_send_messages with a list of the return values of this method.
    """
    if len(content) > MAX_MESSAGE_LENGTH:
        content = content[0:3900] + "\n\n[message was too long and has been truncated]"

    sender = get_user_profile_by_email(sender_email)
    if realm is None:
        raise RuntimeError("None is not a valid realm for internal_prep_message!")
    parsed_recipients = extract_recipients(recipients)
    if recipient_type_name == "stream":
        stream, _ = create_stream_if_needed(realm, parsed_recipients[0])

    try:
        return check_message(sender, get_client("Internal"), recipient_type_name,
                             parsed_recipients, subject, content, realm=realm)
    except JsonableError as e:
        logging.error("Error queueing internal message by %s: %s" % (sender_email, str(e)))

    return None

def internal_send_message(realm, sender_email, recipient_type_name, recipients,
                          subject, content):
    # type: (Realm, Text, str, Text, Text, Text) -> None
    msg = internal_prep_message(realm, sender_email, recipient_type_name, recipients,
                                subject, content)

    # internal_prep_message encountered an error
    if msg is None:
        return

    do_send_messages([msg])

def pick_color(user_profile):
    # type: (UserProfile) -> Text
    subs = Subscription.objects.filter(user_profile=user_profile,
                                       active=True,
                                       recipient__type=Recipient.STREAM)
    return pick_color_helper(user_profile, subs)

def pick_color_helper(user_profile, subs):
    # type: (UserProfile, Iterable[Subscription]) -> Text
    # These colors are shared with the palette in subs.js.
    used_colors = [sub.color for sub in subs if sub.active]
    available_colors = [s for s in STREAM_ASSIGNMENT_COLORS if s not in used_colors]

    if available_colors:
        return available_colors[0]
    else:
        return STREAM_ASSIGNMENT_COLORS[len(used_colors) % len(STREAM_ASSIGNMENT_COLORS)]

def validate_user_access_to_subscribers(user_profile, stream):
    # type: (Optional[UserProfile], Stream) -> None
    """ Validates whether the user can view the subscribers of a stream.  Raises a JsonableError if:
        * The user and the stream are in different realms
        * The realm is MIT and the stream is not invite only.
        * The stream is invite only, requesting_user is passed, and that user
          does not subscribe to the stream.
    """
    validate_user_access_to_subscribers_helper(
        user_profile,
        {"realm__domain": stream.realm.domain,
         "realm_id": stream.realm_id,
         "invite_only": stream.invite_only},
        # We use a lambda here so that we only compute whether the
        # user is subscribed if we have to
        lambda: subscribed_to_stream(user_profile, stream))

def validate_user_access_to_subscribers_helper(user_profile, stream_dict, check_user_subscribed):
    # type: (Optional[UserProfile], Mapping[str, Any], Callable[[], bool]) -> None
    """ Helper for validate_user_access_to_subscribers that doesn't require a full stream object
    * check_user_subscribed is a function that when called with no
      arguments, will report whether the user is subscribed to the stream
    """
    if user_profile is None:
        raise ValidationError("Missing user to validate access for")

    if user_profile.realm_id != stream_dict["realm_id"]:
        raise ValidationError("Requesting user not in given realm")

    if user_profile.realm.is_zephyr_mirror_realm and not stream_dict["invite_only"]:
        raise JsonableError(_("You cannot get subscribers for public streams in this realm"))

    if (stream_dict["invite_only"] and not check_user_subscribed()):
        raise JsonableError(_("Unable to retrieve subscribers for invite-only stream"))

# sub_dict is a dictionary mapping stream_id => whether the user is subscribed to that stream
def bulk_get_subscriber_user_ids(stream_dicts, user_profile, sub_dict):
    # type: (Iterable[Mapping[str, Any]], UserProfile, Mapping[int, bool]) -> Dict[int, List[int]]
    target_stream_dicts = []
    for stream_dict in stream_dicts:
        try:
            validate_user_access_to_subscribers_helper(user_profile, stream_dict,
                                                       lambda: sub_dict[stream_dict["id"]])
        except JsonableError:
            continue
        target_stream_dicts.append(stream_dict)

    subscriptions = Subscription.objects.select_related("recipient").filter(
        recipient__type=Recipient.STREAM,
        recipient__type_id__in=[stream["id"] for stream in target_stream_dicts],
        user_profile__is_active=True,
        active=True).values("user_profile_id", "recipient__type_id")

    result = dict((stream["id"], []) for stream in stream_dicts) # type: Dict[int, List[int]]
    for sub in subscriptions:
        result[sub["recipient__type_id"]].append(sub["user_profile_id"])

    return result

def get_subscribers_query(stream, requesting_user):
    # type: (Stream, UserProfile) -> QuerySet
    # TODO: Make a generic stub for QuerySet
    """ Build a query to get the subscribers list for a stream, raising a JsonableError if:

    'realm' is optional in stream.

    The caller can refine this query with select_related(), values(), etc. depending
    on whether it wants objects or just certain fields
    """
    validate_user_access_to_subscribers(requesting_user, stream)

    # Note that non-active users may still have "active" subscriptions, because we
    # want to be able to easily reactivate them with their old subscriptions.  This
    # is why the query here has to look at the UserProfile.is_active flag.
    subscriptions = Subscription.objects.filter(recipient__type=Recipient.STREAM,
                                                recipient__type_id=stream.id,
                                                user_profile__is_active=True,
                                                active=True)
    return subscriptions

def get_subscribers(stream, requesting_user=None):
    # type: (Stream, Optional[UserProfile]) -> List[UserProfile]
    subscriptions = get_subscribers_query(stream, requesting_user).select_related()
    return [subscription.user_profile for subscription in subscriptions]

def get_subscriber_emails(stream, requesting_user=None):
    # type: (Stream, Optional[UserProfile]) -> List[Text]
    subscriptions_query = get_subscribers_query(stream, requesting_user)
    subscriptions = subscriptions_query.values('user_profile__email')
    return [subscription['user_profile__email'] for subscription in subscriptions]

def maybe_get_subscriber_emails(stream, user_profile):
    # type: (Stream, UserProfile) -> List[Text]
    """ Alternate version of get_subscriber_emails that takes a Stream object only
    (not a name), and simply returns an empty list if unable to get a real
    subscriber list (because we're on the MIT realm). """
    try:
        subscribers = get_subscriber_emails(stream, requesting_user=user_profile)
    except JsonableError:
        subscribers = []
    return subscribers

def notify_subscriptions_added(user_profile, sub_pairs, stream_emails, no_log=False):
    # type: (UserProfile, Iterable[Tuple[Subscription, Stream]], Callable[[Stream], List[Text]], bool) -> None
    if not no_log:
        log_event({'type': 'subscription_added',
                   'user': user_profile.email,
                   'names': [stream.name for sub, stream in sub_pairs],
                   'domain': user_profile.realm.domain})

    # Send a notification to the user who subscribed.
    payload = [dict(name=stream.name,
                    stream_id=stream.id,
                    in_home_view=subscription.in_home_view,
                    invite_only=stream.invite_only,
                    color=subscription.color,
                    email_address=encode_email_address(stream),
                    desktop_notifications=subscription.desktop_notifications,
                    audible_notifications=subscription.audible_notifications,
                    description=stream.description,
                    pin_to_top=subscription.pin_to_top,
                    subscribers=stream_emails(stream))
               for (subscription, stream) in sub_pairs]
    event = dict(type="subscription", op="add",
                 subscriptions=payload)
    send_event(event, [user_profile.id])

def get_peer_user_ids_for_stream_change(stream, altered_users, subscribed_users):
    # type: (Stream, Iterable[UserProfile], Iterable[UserProfile]) -> Set[int]
    '''
    altered_users is a list of users that we are adding/removing
    subscribed_users is the list of already subscribed users

    Based on stream policy, we notify the correct bystanders, while
    not notifying altered_users (who get subscribers via another event)
    '''

    altered_user_ids = [user.id for user in altered_users]

    if stream.invite_only:
        # PRIVATE STREAMS
        all_subscribed_ids = [user.id for user in subscribed_users]
        return set(all_subscribed_ids) - set(altered_user_ids)

    else:
        # PUBLIC STREAMS
        # We now do "peer_add" or "peer_remove" events even for streams
        # users were never subscribed to, in order for the neversubscribed
        # structure to stay up-to-date.
        return set(active_user_ids(stream.realm)) - set(altered_user_ids)

def query_all_subs_by_stream(streams):
    # type: (Iterable[Stream]) -> Dict[int, List[UserProfile]]
    all_subs = Subscription.objects.filter(recipient__type=Recipient.STREAM,
                                           recipient__type_id__in=[stream.id for stream in streams],
                                           user_profile__is_active=True,
                                           active=True).select_related('recipient', 'user_profile')

    all_subs_by_stream = defaultdict(list) # type: Dict[int, List[UserProfile]]
    for sub in all_subs:
        all_subs_by_stream[sub.recipient.type_id].append(sub.user_profile)
    return all_subs_by_stream

def bulk_add_subscriptions(streams, users):
    # type: (Iterable[Stream], Iterable[UserProfile]) -> Tuple[List[Tuple[UserProfile, Stream]], List[Tuple[UserProfile, Stream]]]
    recipients_map = bulk_get_recipients(Recipient.STREAM, [stream.id for stream in streams]) # type: Mapping[int, Recipient]
    recipients = [recipient.id for recipient in recipients_map.values()] # type: List[int]

    stream_map = {} # type: Dict[int, Stream]
    for stream in streams:
        stream_map[recipients_map[stream.id].id] = stream

    subs_by_user = defaultdict(list) # type: Dict[int, List[Subscription]]
    all_subs_query = Subscription.objects.select_related("user_profile")
    for sub in all_subs_query.filter(user_profile__in=users,
                                     recipient__type=Recipient.STREAM):
        subs_by_user[sub.user_profile_id].append(sub)

    already_subscribed = [] # type: List[Tuple[UserProfile, Stream]]
    subs_to_activate = [] # type: List[Tuple[Subscription, Stream]]
    new_subs = [] # type: List[Tuple[UserProfile, int, Stream]]
    for user_profile in users:
        needs_new_sub = set(recipients) # type: Set[int]
        for sub in subs_by_user[user_profile.id]:
            if sub.recipient_id in needs_new_sub:
                needs_new_sub.remove(sub.recipient_id)
                if sub.active:
                    already_subscribed.append((user_profile, stream_map[sub.recipient_id]))
                else:
                    subs_to_activate.append((sub, stream_map[sub.recipient_id]))
                    # Mark the sub as active, without saving, so that
                    # pick_color will consider this to be an active
                    # subscription when picking colors
                    sub.active = True
        for recipient_id in needs_new_sub:
            new_subs.append((user_profile, recipient_id, stream_map[recipient_id]))

    subs_to_add = [] # type: List[Tuple[Subscription, Stream]]
    for (user_profile, recipient_id, stream) in new_subs:
        color = pick_color_helper(user_profile, subs_by_user[user_profile.id])
        sub_to_add = Subscription(user_profile=user_profile, active=True,
                                  color=color, recipient_id=recipient_id,
                                  desktop_notifications=user_profile.enable_stream_desktop_notifications,
                                  audible_notifications=user_profile.enable_stream_sounds)
        subs_by_user[user_profile.id].append(sub_to_add)
        subs_to_add.append((sub_to_add, stream))

    # TODO: XXX: This transaction really needs to be done at the serializeable
    # transaction isolation level.
    with transaction.atomic():
        occupied_streams_before = list(get_occupied_streams(user_profile.realm))
        Subscription.objects.bulk_create([sub for (sub, stream) in subs_to_add])
        Subscription.objects.filter(id__in=[sub.id for (sub, stream) in subs_to_activate]).update(active=True)
        occupied_streams_after = list(get_occupied_streams(user_profile.realm))

    new_occupied_streams = [stream for stream in
                            set(occupied_streams_after) - set(occupied_streams_before)
                            if not stream.invite_only]
    if new_occupied_streams:
        event = dict(type="stream", op="occupy",
                     streams=[stream.to_dict()
                              for stream in new_occupied_streams])
        send_event(event, active_user_ids(user_profile.realm))

    # Notify all existing users on streams that users have joined

    # First, get all users subscribed to the streams that we care about
    # We fetch all subscription information upfront, as it's used throughout
    # the following code and we want to minize DB queries
    all_subs_by_stream = query_all_subs_by_stream(streams=streams)

    def fetch_stream_subscriber_emails(stream):
        # type: (Stream) -> List[Text]
        if stream.realm.is_zephyr_mirror_realm and not stream.invite_only:
            return []
        users = all_subs_by_stream[stream.id]
        return [u.email for u in users]

    sub_tuples_by_user = defaultdict(list) # type: Dict[int, List[Tuple[Subscription, Stream]]]
    new_streams = set() # type: Set[Tuple[int, int]]
    for (sub, stream) in subs_to_add + subs_to_activate:
        sub_tuples_by_user[sub.user_profile.id].append((sub, stream))
        new_streams.add((sub.user_profile.id, stream.id))

    # We now send several types of events to notify browsers.  The
    # first batch is notifications to users on invite-only streams
    # that the stream exists.
    for stream in streams:
        new_users = [user for user in users if (user.id, stream.id) in new_streams]

        # Users newly added to invite-only streams need a `create`
        # notification, since they didn't have the invite-only stream
        # in their browser yet.
        if stream.invite_only:
            event = dict(type="stream", op="create",
                         streams=[stream.to_dict()])
            send_event(event, [user.id for user in new_users])

    # The second batch is events for the users themselves that they
    # were subscribed to the new streams.
    for user_profile in users:
        if len(sub_tuples_by_user[user_profile.id]) == 0:
            continue
        sub_pairs = sub_tuples_by_user[user_profile.id]
        notify_subscriptions_added(user_profile, sub_pairs, fetch_stream_subscriber_emails)

    # The second batch is events for other users who are tracking the
    # subscribers lists of streams in their browser; everyone for
    # public streams and only existing subscribers for private streams.
    for stream in streams:
        if stream.realm.is_zephyr_mirror_realm and not stream.invite_only:
            continue

        new_users = [user for user in users if (user.id, stream.id) in new_streams]

        peer_user_ids = get_peer_user_ids_for_stream_change(
            stream=stream,
            altered_users=new_users,
            subscribed_users=all_subs_by_stream[stream.id]
        )

        if peer_user_ids:
            for added_user in new_users:
                event = dict(type="subscription", op="peer_add",
                             subscriptions=[stream.name],
                             user_id=added_user.id)
                send_event(event, peer_user_ids)

    return ([(user_profile, stream) for (user_profile, recipient_id, stream) in new_subs] +
            [(sub.user_profile, stream) for (sub, stream) in subs_to_activate],
            already_subscribed)

def notify_subscriptions_removed(user_profile, streams, no_log=False):
    # type: (UserProfile, Iterable[Stream], bool) -> None
    if not no_log:
        log_event({'type': 'subscription_removed',
                   'user': user_profile.email,
                   'names': [stream.name for stream in streams],
                   'domain': user_profile.realm.domain})

    payload = [dict(name=stream.name, stream_id=stream.id) for stream in streams]
    event = dict(type="subscription", op="remove",
                 subscriptions=payload)
    send_event(event, [user_profile.id])

def bulk_remove_subscriptions(users, streams):
    # type: (Iterable[UserProfile], Iterable[Stream]) -> Tuple[List[Tuple[UserProfile, Stream]], List[Tuple[UserProfile, Stream]]]

    recipients_map = bulk_get_recipients(Recipient.STREAM,
                                         [stream.id for stream in streams]) # type: Mapping[int, Recipient]
    stream_map = {} # type: Dict[int, Stream]
    for stream in streams:
        stream_map[recipients_map[stream.id].id] = stream

    subs_by_user = dict((user_profile.id, []) for user_profile in users) # type: Dict[int, List[Subscription]]
    for sub in Subscription.objects.select_related("user_profile").filter(user_profile__in=users,
                                                                          recipient__in=list(recipients_map.values()),
                                                                          active=True):
        subs_by_user[sub.user_profile_id].append(sub)

    subs_to_deactivate = [] # type: List[Tuple[Subscription, Stream]]
    not_subscribed = [] # type: List[Tuple[UserProfile, Stream]]
    for user_profile in users:
        recipients_to_unsub = set([recipient.id for recipient in recipients_map.values()])
        for sub in subs_by_user[user_profile.id]:
            recipients_to_unsub.remove(sub.recipient_id)
            subs_to_deactivate.append((sub, stream_map[sub.recipient_id]))
        for recipient_id in recipients_to_unsub:
            not_subscribed.append((user_profile, stream_map[recipient_id]))

    # TODO: XXX: This transaction really needs to be done at the serializeable
    # transaction isolation level.
    with transaction.atomic():
        occupied_streams_before = list(get_occupied_streams(user_profile.realm))
        Subscription.objects.filter(id__in=[sub.id for (sub, stream_name) in
                                            subs_to_deactivate]).update(active=False)
        occupied_streams_after = list(get_occupied_streams(user_profile.realm))

    new_vacant_streams = [stream for stream in
                          set(occupied_streams_before) - set(occupied_streams_after)
                          if not stream.invite_only]
    if new_vacant_streams:
        event = dict(type="stream", op="vacate",
                     streams=[stream.to_dict()
                              for stream in new_vacant_streams])
        send_event(event, active_user_ids(user_profile.realm))

    altered_user_dict = defaultdict(list) # type: Dict[int, List[UserProfile]]
    streams_by_user = defaultdict(list) # type: Dict[int, List[Stream]]
    for (sub, stream) in subs_to_deactivate:
        streams_by_user[sub.user_profile_id].append(stream)
        altered_user_dict[stream.id].append(sub.user_profile)

    for user_profile in users:
        if len(streams_by_user[user_profile.id]) == 0:
            continue
        notify_subscriptions_removed(user_profile, streams_by_user[user_profile.id])

    all_subs_by_stream = query_all_subs_by_stream(streams=streams)

    for stream in streams:
        if stream.realm.is_zephyr_mirror_realm and not stream.invite_only:
            continue

        altered_users = altered_user_dict[stream.id]

        peer_user_ids = get_peer_user_ids_for_stream_change(
            stream=stream,
            altered_users=altered_users,
            subscribed_users=all_subs_by_stream[stream.id]
        )

        if peer_user_ids:
            for removed_user in altered_users:
                event = dict(type="subscription",
                             op="peer_remove",
                             subscriptions=[stream.name],
                             user_id=removed_user.id)
                send_event(event, peer_user_ids)

    return ([(sub.user_profile, stream) for (sub, stream) in subs_to_deactivate],
            not_subscribed)

def log_subscription_property_change(user_email, stream_name, property, value):
    # type: (Text, Text, Text, Any) -> None
    event = {'type': 'subscription_property',
             'property': property,
             'user': user_email,
             'stream_name': stream_name,
             'value': value}
    log_event(event)

def do_change_subscription_property(user_profile, sub, stream_name,
                                    property_name, value):
    # type: (UserProfile, Subscription, Text, Text, Any) -> None
    setattr(sub, property_name, value)
    sub.save(update_fields=[property_name])
    log_subscription_property_change(user_profile.email, stream_name,
                                     property_name, value)

    event = dict(type="subscription",
                 op="update",
                 email=user_profile.email,
                 property=property_name,
                 value=value,
                 name=stream_name)
    send_event(event, [user_profile.id])

def do_activate_user(user_profile, log=True, join_date=timezone.now()):
    # type: (UserProfile, bool, datetime.datetime) -> None
    user_profile.is_active = True
    user_profile.is_mirror_dummy = False
    user_profile.set_unusable_password()
    user_profile.date_joined = join_date
    user_profile.tos_version = settings.TOS_VERSION
    user_profile.save(update_fields=["is_active", "date_joined", "password",
                                     "is_mirror_dummy", "tos_version"])

    if log:
        domain = user_profile.realm.domain
        log_event({'type': 'user_activated',
                   'user': user_profile.email,
                   'domain': domain})

    notify_created_user(user_profile)

def do_reactivate_user(user_profile):
    # type: (UserProfile) -> None
    # Unlike do_activate_user, this is meant for re-activating existing users,
    # so it doesn't reset their password, etc.
    user_profile.is_active = True
    user_profile.save(update_fields=["is_active"])

    domain = user_profile.realm.domain
    log_event({'type': 'user_reactivated',
               'user': user_profile.email,
               'domain': domain})

    notify_created_user(user_profile)

def do_change_password(user_profile, password, log=True, commit=True,
                       hashed_password=False):
    # type: (UserProfile, Text, bool, bool, bool) -> None
    if hashed_password:
        # This is a hashed password, not the password itself.
        user_profile.set_password(password)
    else:
        user_profile.set_password(password)
    if commit:
        user_profile.save(update_fields=["password"])
    if log:
        log_event({'type': 'user_change_password',
                   'user': user_profile.email,
                   'pwhash': user_profile.password})

def do_change_full_name(user_profile, full_name, log=True):
    # type: (UserProfile, Text, bool) -> None
    user_profile.full_name = full_name
    user_profile.save(update_fields=["full_name"])
    if log:
        log_event({'type': 'user_change_full_name',
                   'user': user_profile.email,
                   'full_name': full_name})

    payload = dict(email=user_profile.email,
                   user_id=user_profile.id,
                   full_name=user_profile.full_name)
    send_event(dict(type='realm_user', op='update', person=payload),
               active_user_ids(user_profile.realm))
    if user_profile.is_bot:
        send_event(dict(type='realm_bot', op='update', bot=payload),
                   bot_owner_userids(user_profile))

def do_change_tos_version(user_profile, tos_version, log=True):
    # type: (UserProfile, Text, bool) -> None
    user_profile.tos_version = tos_version
    user_profile.save(update_fields=["tos_version"])
    if log:
        log_event({'type': 'user_change_tos_version',
                   'user': user_profile.email,
                   'tos_version': tos_version})

def do_regenerate_api_key(user_profile, log=True):
    # type: (UserProfile, bool) -> None
    user_profile.api_key = random_api_key()
    user_profile.save(update_fields=["api_key"])

    if log:
        log_event({'type': 'user_change_api_key',
                   'user': user_profile.email})

    if user_profile.is_bot:
        send_event(dict(type='realm_bot',
                        op='update',
                        bot=dict(email=user_profile.email,
                                 user_id=user_profile.id,
                                 api_key=user_profile.api_key,
                                 )),
                   bot_owner_userids(user_profile))

def do_change_avatar_source(user_profile, avatar_source, log=True):
    # type: (UserProfile, Text, bool) -> None
    user_profile.avatar_source = avatar_source
    user_profile.save(update_fields=["avatar_source"])

    if log:
        log_event({'type': 'user_change_avatar_source',
                   'user': user_profile.email,
                   'avatar_source': avatar_source})

    if user_profile.is_bot:
        send_event(dict(type='realm_bot',
                        op='update',
                        bot=dict(email=user_profile.email,
                                 user_id=user_profile.id,
                                 avatar_url=avatar_url(user_profile),
                                 )),
                   bot_owner_userids(user_profile))
    else:
        payload = dict(
            email=user_profile.email,
            avatar_url=avatar_url(user_profile),
            user_id=user_profile.id
        )

        send_event(dict(type='realm_user',
                        op='update',
                        person=payload),
                   active_user_ids(user_profile.realm))

def _default_stream_permision_check(user_profile, stream):
    # type: (UserProfile, Optional[Stream]) -> None
    # Any user can have a None default stream
    if stream is not None:
        if user_profile.is_bot:
            user = user_profile.bot_owner
        else:
            user = user_profile
        if stream.invite_only and not subscribed_to_stream(user, stream):
            raise JsonableError(_('Insufficient permission'))

def do_change_default_sending_stream(user_profile, stream, log=True):
    # type: (UserProfile, Stream, bool) -> None
    _default_stream_permision_check(user_profile, stream)

    user_profile.default_sending_stream = stream
    user_profile.save(update_fields=['default_sending_stream'])
    if log:
        log_event({'type': 'user_change_default_sending_stream',
                   'user': user_profile.email,
                   'stream': str(stream)})
    if user_profile.is_bot:
        if stream:
            stream_name = stream.name
        else:
            stream_name = None
        send_event(dict(type='realm_bot',
                        op='update',
                        bot=dict(email=user_profile.email,
                                 user_id=user_profile.id,
                                 default_sending_stream=stream_name,
                                 )),
                   bot_owner_userids(user_profile))

def do_change_default_events_register_stream(user_profile, stream, log=True):
    # type: (UserProfile, Stream, bool) -> None
    _default_stream_permision_check(user_profile, stream)

    user_profile.default_events_register_stream = stream
    user_profile.save(update_fields=['default_events_register_stream'])
    if log:
        log_event({'type': 'user_change_default_events_register_stream',
                   'user': user_profile.email,
                   'stream': str(stream)})
    if user_profile.is_bot:
        if stream:
            stream_name = stream.name
        else:
            stream_name = None
        send_event(dict(type='realm_bot',
                        op='update',
                        bot=dict(email=user_profile.email,
                                 user_id=user_profile.id,
                                 default_events_register_stream=stream_name,
                                 )),
                   bot_owner_userids(user_profile))

def do_change_default_all_public_streams(user_profile, value, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.default_all_public_streams = value
    user_profile.save(update_fields=['default_all_public_streams'])
    if log:
        log_event({'type': 'user_change_default_all_public_streams',
                   'user': user_profile.email,
                   'value': str(value)})
    if user_profile.is_bot:
        send_event(dict(type='realm_bot',
                        op='update',
                        bot=dict(email=user_profile.email,
                                 user_id=user_profile.id,
                                 default_all_public_streams=user_profile.default_all_public_streams,
                                 )),
                   bot_owner_userids(user_profile))

def do_change_is_admin(user_profile, value, permission='administer'):
    # type: (UserProfile, bool, str) -> None
    if permission == "administer":
        user_profile.is_realm_admin = value
        user_profile.save(update_fields=["is_realm_admin"])
    elif permission == "api_super_user":
        user_profile.is_api_super_user = value
        user_profile.save(update_fields=["is_api_super_user"])
    else:
        raise Exception("Unknown permission")

    if permission == 'administer':
        event = dict(type="realm_user", op="update",
                     person=dict(email=user_profile.email,
                                 user_id=user_profile.id,
                                 is_admin=value))
        send_event(event, active_user_ids(user_profile.realm))

def do_change_bot_type(user_profile, value):
    # type: (UserProfile, int) -> None
    user_profile.bot_type = value
    user_profile.save(update_fields=["bot_type"])

def do_change_stream_invite_only(stream, invite_only):
    # type: (Stream, bool) -> None
    stream.invite_only = invite_only
    stream.save(update_fields=['invite_only'])

def do_rename_stream(stream, new_name, log=True):
    # type: (Stream, Text, bool) -> Dict[str, Text]
    old_name = stream.name
    stream.name = new_name
    stream.save(update_fields=["name"])

    if log:
        log_event({'type': 'stream_name_change',
                   'domain': stream.realm.domain,
                   'new_name': new_name})

    recipient = get_recipient(Recipient.STREAM, stream.id)
    messages = Message.objects.filter(recipient=recipient).only("id")

    # Update the display recipient and stream, which are easy single
    # items to set.
    old_cache_key = get_stream_cache_key(old_name, stream.realm)
    new_cache_key = get_stream_cache_key(stream.name, stream.realm)
    if old_cache_key != new_cache_key:
        cache_delete(old_cache_key)
        cache_set(new_cache_key, stream)
    cache_set(display_recipient_cache_key(recipient.id), stream.name)

    # Delete cache entries for everything else, which is cheaper and
    # clearer than trying to set them. display_recipient is the out of
    # date field in all cases.
    cache_delete_many(
        to_dict_cache_key_id(message.id, True) for message in messages)
    cache_delete_many(
        to_dict_cache_key_id(message.id, False) for message in messages)
    new_email = encode_email_address(stream)

    # We will tell our users to essentially
    # update stream.name = new_name where name = old_name
    # and update stream.email = new_email where name = old_name.
    # We could optimize this by trying to send one message, but the
    # client code really wants one property update at a time, and
    # updating stream names is a pretty infrequent operation.
    # More importantly, we want to key these updates by id, not name,
    # since id is the immutable primary key, and obviously name is not.
    data_updates = [
        ['email_address', new_email],
        ['name', new_name],
    ]
    for property, value in data_updates:
        event = dict(
            op="update",
            type="stream",
            property=property,
            value=value,
            name=old_name
        )
        send_event(event, can_access_stream_user_ids(stream))

    # Even though the token doesn't change, the web client needs to update the
    # email forwarding address to display the correctly-escaped new name.
    return {"email_address": new_email}

def do_change_stream_description(stream, new_description):
    # type: (Stream, Text) -> None
    stream.description = new_description
    stream.save(update_fields=['description'])

    event = dict(type='stream', op='update',
                 property='description', name=stream.name,
                 value=new_description)
    send_event(event, can_access_stream_user_ids(stream))

def do_create_realm(string_id, name, restricted_to_domain=None,
                    invite_required=None, org_type=None):
    # type: (Text, Text, Optional[bool], Optional[bool], Optional[int]) -> Tuple[Realm, bool]
    realm = get_realm(string_id)
    created = not realm
    if created:
        kwargs = {} # type: Dict[str, Any]
        if restricted_to_domain is not None:
            kwargs['restricted_to_domain'] = restricted_to_domain
        if invite_required is not None:
            kwargs['invite_required'] = invite_required
        if org_type is not None:
            kwargs['org_type'] = org_type
        realm = Realm(string_id=string_id, name=name,
                      domain=string_id + '@acme.com', **kwargs)
        realm.save()

        # Create stream once Realm object has been saved
        notifications_stream, _ = create_stream_if_needed(realm, Realm.DEFAULT_NOTIFICATION_STREAM_NAME)
        realm.notifications_stream = notifications_stream
        realm.save(update_fields=['notifications_stream'])

        # Include a welcome message in this notifications stream
        product_name = "Zulip"
        content = """Hello, and welcome to %s!

This is a message on stream `%s` with the topic `welcome`. We'll use this stream for
system-generated notifications.""" % (product_name, notifications_stream.name,)
        msg = internal_prep_message(realm, settings.WELCOME_BOT, 'stream',
                                    notifications_stream.name, "welcome",
                                    content)
        do_send_messages([msg])

        # Log the event
        log_event({"type": "realm_created",
                   "string_id": string_id,
                   "restricted_to_domain": restricted_to_domain,
                   "invite_required": invite_required,
                   "org_type": org_type})

        # Send a notification to the admin realm (if configured)
        if settings.NEW_USER_BOT is not None:
            signup_message = "Signups enabled"
            admin_realm = get_user_profile_by_email(settings.NEW_USER_BOT).realm
            internal_send_message(admin_realm, settings.NEW_USER_BOT, "stream",
                                  "signups", string_id, signup_message)
    return (realm, created)

def do_change_enable_stream_desktop_notifications(user_profile,
                                                  enable_stream_desktop_notifications,
                                                  log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_stream_desktop_notifications = enable_stream_desktop_notifications
    user_profile.save(update_fields=["enable_stream_desktop_notifications"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_stream_desktop_notifications',
             'setting': enable_stream_desktop_notifications}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_enable_stream_sounds(user_profile, enable_stream_sounds, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_stream_sounds = enable_stream_sounds
    user_profile.save(update_fields=["enable_stream_sounds"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_stream_sounds',
             'setting': enable_stream_sounds}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_enable_desktop_notifications(user_profile, enable_desktop_notifications, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_desktop_notifications = enable_desktop_notifications
    user_profile.save(update_fields=["enable_desktop_notifications"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_desktop_notifications',
             'setting': enable_desktop_notifications}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_pm_content_in_desktop_notifications(user_profile,
                                                  pm_content_in_desktop_notifications, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.pm_content_in_desktop_notifications \
        = pm_content_in_desktop_notifications
    user_profile.save(update_fields=["pm_content_in_desktop_notifications"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'pm_content_in_desktop_notifications',
             'setting': pm_content_in_desktop_notifications}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])


def do_change_enable_sounds(user_profile, enable_sounds, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_sounds = enable_sounds
    user_profile.save(update_fields=["enable_sounds"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_sounds',
             'setting': enable_sounds}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_enable_offline_email_notifications(user_profile, offline_email_notifications, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_offline_email_notifications = offline_email_notifications
    user_profile.save(update_fields=["enable_offline_email_notifications"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_offline_email_notifications',
             'setting': offline_email_notifications}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_enable_offline_push_notifications(user_profile, offline_push_notifications, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_offline_push_notifications = offline_push_notifications
    user_profile.save(update_fields=["enable_offline_push_notifications"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_offline_push_notifications',
             'setting': offline_push_notifications}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_enable_online_push_notifications(user_profile, enable_online_push_notifications, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_online_push_notifications = enable_online_push_notifications
    user_profile.save(update_fields=["enable_online_push_notifications"])
    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_online_push_notifications',
             'setting': enable_online_push_notifications}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_enable_digest_emails(user_profile, enable_digest_emails, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.enable_digest_emails = enable_digest_emails
    user_profile.save(update_fields=["enable_digest_emails"])

    if not enable_digest_emails:
        # Remove any digest emails that have been enqueued.
        clear_followup_emails_queue(user_profile.email)

    event = {'type': 'update_global_notifications',
             'user': user_profile.email,
             'notification_name': 'enable_digest_emails',
             'setting': enable_digest_emails}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_autoscroll_forever(user_profile, autoscroll_forever, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.autoscroll_forever = autoscroll_forever
    user_profile.save(update_fields=["autoscroll_forever"])

    if log:
        log_event({'type': 'autoscroll_forever',
                   'user': user_profile.email,
                   'autoscroll_forever': autoscroll_forever})

def do_change_enter_sends(user_profile, enter_sends):
    # type: (UserProfile, bool) -> None
    user_profile.enter_sends = enter_sends
    user_profile.save(update_fields=["enter_sends"])

def do_change_default_desktop_notifications(user_profile, default_desktop_notifications):
    # type: (UserProfile, bool) -> None
    user_profile.default_desktop_notifications = default_desktop_notifications
    user_profile.save(update_fields=["default_desktop_notifications"])

def do_change_twenty_four_hour_time(user_profile, setting_value, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.twenty_four_hour_time = setting_value
    user_profile.save(update_fields=["twenty_four_hour_time"])
    event = {'type': 'update_display_settings',
             'user': user_profile.email,
             'setting_name': 'twenty_four_hour_time',
             'setting': setting_value}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_left_side_userlist(user_profile, setting_value, log=True):
    # type: (UserProfile, bool, bool) -> None
    user_profile.left_side_userlist = setting_value
    user_profile.save(update_fields=["left_side_userlist"])
    event = {'type': 'update_display_settings',
             'user': user_profile.email,
             'setting_name': 'left_side_userlist',
             'setting': setting_value}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def do_change_default_language(user_profile, setting_value, log=True):
    # type: (UserProfile, Text, bool) -> None

    user_profile.default_language = setting_value
    user_profile.save(update_fields=["default_language"])
    event = {'type': 'update_display_settings',
             'user': user_profile.email,
             'setting_name': 'default_language',
             'setting': setting_value}
    if log:
        log_event(event)
    send_event(event, [user_profile.id])

def set_default_streams(realm, stream_dict):
    # type: (Realm, Dict[Text, Dict[Text, Any]]) -> None
    DefaultStream.objects.filter(realm=realm).delete()
    stream_names = []
    for name, options in stream_dict.items():
        stream_names.append(name)
        stream, _ = create_stream_if_needed(realm,
                                            name,
                                            invite_only = options["invite_only"],
                                            stream_description = options["description"])
        DefaultStream.objects.create(stream=stream, realm=realm)

    # Always include the realm's default notifications streams, if it exists
    if realm.notifications_stream is not None:
        DefaultStream.objects.get_or_create(stream=realm.notifications_stream, realm=realm)

    log_event({'type': 'default_streams',
               'domain': realm.domain,
               'streams': stream_names})

def notify_default_streams(realm):
    # type: (Realm) -> None
    event = dict(
        type="default_streams",
        default_streams=streams_to_dicts_sorted(get_default_streams_for_realm(realm))
    )
    send_event(event, active_user_ids(realm))

def do_add_default_stream(stream):
    # type: (Stream) -> None
    if not DefaultStream.objects.filter(realm=stream.realm, stream=stream).exists():
        DefaultStream.objects.create(realm=stream.realm, stream=stream)
        notify_default_streams(stream.realm)

def do_remove_default_stream(stream):
    # type: (Stream) -> None
    DefaultStream.objects.filter(realm=stream.realm, stream=stream).delete()
    notify_default_streams(stream.realm)

def get_default_streams_for_realm(realm):
    # type: (Realm) -> List[Stream]
    return [default.stream for default in
            DefaultStream.objects.select_related("stream", "stream__realm").filter(realm=realm)]

def get_default_subs(user_profile):
    # type: (UserProfile) -> List[Stream]
    # Right now default streams are realm-wide.  This wrapper gives us flexibility
    # to some day further customize how we set up default streams for new users.
    return get_default_streams_for_realm(user_profile.realm)

# returns default streams in json serializeable format
def streams_to_dicts_sorted(streams):
    # type: (List[Stream]) -> List[Dict[str, Any]]
    return sorted([stream.to_dict() for stream in streams], key=lambda elt: elt["name"])

def do_update_user_activity_interval(user_profile, log_time):
    # type: (UserProfile, datetime.datetime) -> None
    effective_end = log_time + datetime.timedelta(minutes=15)
    # This code isn't perfect, because with various races we might end
    # up creating two overlapping intervals, but that shouldn't happen
    # often, and can be corrected for in post-processing
    try:
        last = UserActivityInterval.objects.filter(user_profile=user_profile).order_by("-end")[0]
        # There are two ways our intervals could overlap:
        # (1) The start of the new interval could be inside the old interval
        # (2) The end of the new interval could be inside the old interval
        # In either case, we just extend the old interval to include the new interval.
        if ((log_time <= last.end and log_time >= last.start) or
                (effective_end <= last.end and effective_end >= last.start)):
            last.end = max(last.end, effective_end)
            last.start = min(last.start, log_time)
            last.save(update_fields=["start", "end"])
            return
    except IndexError:
        pass

    # Otherwise, the intervals don't overlap, so we should make a new one
    UserActivityInterval.objects.create(user_profile=user_profile, start=log_time,
                                        end=effective_end)

@statsd_increment('user_activity')
def do_update_user_activity(user_profile, client, query, log_time):
    # type: (UserProfile, Client, Text, datetime.datetime) -> None
    (activity, created) = UserActivity.objects.get_or_create(
        user_profile = user_profile,
        client = client,
        query = query,
        defaults={'last_visit': log_time, 'count': 0})

    activity.count += 1
    activity.last_visit = log_time
    activity.save(update_fields=["last_visit", "count"])

def send_presence_changed(user_profile, presence):
    # type: (UserProfile, UserPresence) -> None
    presence_dict = presence.to_dict()
    event = dict(type="presence", email=user_profile.email,
                 server_timestamp=time.time(),
                 presence={presence_dict['client']: presence.to_dict()})
    send_event(event, active_user_ids(user_profile.realm))

def consolidate_client(client):
    # type: (Client) -> Client
    # The web app reports a client as 'website'
    # The desktop app reports a client as ZulipDesktop
    # due to it setting a custom user agent. We want both
    # to count as web users

    # Alias ZulipDesktop to website
    if client.name in ['ZulipDesktop']:
        return get_client('website')
    else:
        return client

@statsd_increment('user_presence')
def do_update_user_presence(user_profile, client, log_time, status):
    # type: (UserProfile, Client, datetime.datetime, int) -> None
    client = consolidate_client(client)
    (presence, created) = UserPresence.objects.get_or_create(
        user_profile = user_profile,
        client = client,
        defaults = {'timestamp': log_time,
                    'status': status})

    stale_status = (log_time - presence.timestamp) > datetime.timedelta(minutes=1, seconds=10)
    was_idle = presence.status == UserPresence.IDLE
    became_online = (status == UserPresence.ACTIVE) and (stale_status or was_idle)

    # If an object was created, it has already been saved.
    #
    # We suppress changes from ACTIVE to IDLE before stale_status is reached;
    # this protects us from the user having two clients open: one active, the
    # other idle. Without this check, we would constantly toggle their status
    # between the two states.
    if not created and stale_status or was_idle or status == presence.status:
        # The following block attempts to only update the "status"
        # field in the event that it actually changed.  This is
        # important to avoid flushing the UserPresence cache when the
        # data it would return to a client hasn't actually changed
        # (see the UserPresence post_save hook for details).
        presence.timestamp = log_time
        update_fields = ["timestamp"]
        if presence.status != status:
            presence.status = status
            update_fields.append("status")
        presence.save(update_fields=update_fields)

    if not user_profile.realm.is_zephyr_mirror_realm and (created or became_online):
        # Push event to all users in the realm so they see the new user
        # appear in the presence list immediately, or the newly online
        # user without delay.  Note that we won't send an update here for a
        # timestamp update, because we rely on the browser to ping us every 50
        # seconds for realm-wide status updates, and those updates should have
        # recent timestamps, which means the browser won't think active users
        # have gone idle.  If we were more aggressive in this function about
        # sending timestamp updates, we could eliminate the ping responses, but
        # that's not a high priority for now, considering that most of our non-MIT
        # realms are pretty small.
        send_presence_changed(user_profile, presence)

def update_user_activity_interval(user_profile, log_time):
    # type: (UserProfile, datetime.datetime) -> None
    event = {'user_profile_id': user_profile.id,
             'time': datetime_to_timestamp(log_time)}
    queue_json_publish("user_activity_interval", event,
                       lambda e: do_update_user_activity_interval(user_profile, log_time))

def update_user_presence(user_profile, client, log_time, status,
                         new_user_input):
    # type: (UserProfile, Client, datetime.datetime, int, bool) -> None
    event = {'user_profile_id': user_profile.id,
             'status': status,
             'time': datetime_to_timestamp(log_time),
             'client': client.name}

    queue_json_publish("user_presence", event,
                       lambda e: do_update_user_presence(user_profile, client,
                                                         log_time, status))

    if new_user_input:
        update_user_activity_interval(user_profile, log_time)

def do_update_pointer(user_profile, pointer, update_flags=False):
    # type: (UserProfile, int, bool) -> None
    prev_pointer = user_profile.pointer
    user_profile.pointer = pointer
    user_profile.save(update_fields=["pointer"])

    if update_flags:
        # Until we handle the new read counts in the Android app
        # natively, this is a shim that will mark as read any messages
        # up until the pointer move
        UserMessage.objects.filter(user_profile=user_profile,
                                   message__id__gt=prev_pointer,
                                   message__id__lte=pointer,
                                   flags=~UserMessage.flags.read)        \
                           .update(flags=F('flags').bitor(UserMessage.flags.read))

    event = dict(type='pointer', pointer=pointer)
    send_event(event, [user_profile.id])

def do_update_message_flags(user_profile, operation, flag, messages, all, stream_obj, topic_name):
    # type: (UserProfile, Text, Text, Sequence[int], bool, Optional[Stream], Optional[Text]) -> int
    flagattr = getattr(UserMessage.flags, flag)

    if all:
        log_statsd_event('bankruptcy')
        msgs = UserMessage.objects.filter(user_profile=user_profile)
    elif stream_obj is not None:
        recipient = get_recipient(Recipient.STREAM, stream_obj.id)
        if topic_name:
            msgs = UserMessage.objects.filter(message__recipient=recipient,
                                              user_profile=user_profile,
                                              message__subject__iexact=topic_name)
        else:
            msgs = UserMessage.objects.filter(message__recipient=recipient, user_profile=user_profile)
    else:
        msgs = UserMessage.objects.filter(user_profile=user_profile,
                                          message__id__in=messages)
        # Hack to let you star any message
        if msgs.count() == 0:
            if not len(messages) == 1:
                raise JsonableError(_("Invalid message(s)"))
            if flag != "starred":
                raise JsonableError(_("Invalid message(s)"))
            # Validate that the user could have read the relevant message
            message = access_message(user_profile, messages[0])[0]

            # OK, this is a message that you legitimately have access
            # to via narrowing to the stream it is on, even though you
            # didn't actually receive it.  So we create a historical,
            # read UserMessage message row for you to star.
            UserMessage.objects.create(user_profile=user_profile,
                                       message=message,
                                       flags=UserMessage.flags.historical | UserMessage.flags.read)

    # The filter() statements below prevent postgres from doing a lot of
    # unnecessary work, which is a big deal for users updating lots of
    # flags (e.g. bankruptcy).  This patch arose from seeing slow calls
    # to POST /json/messages/flags in the logs.  The filter() statements
    # are kind of magical; they are actually just testing the one bit.
    if operation == 'add':
        msgs = msgs.filter(flags=~flagattr)
        if stream_obj:
            messages = list(msgs.values_list('message__id', flat=True))
        count = msgs.update(flags=F('flags').bitor(flagattr))
    elif operation == 'remove':
        msgs = msgs.filter(flags=flagattr)
        if stream_obj:
            messages = list(msgs.values_list('message__id', flat=True))
        count = msgs.update(flags=F('flags').bitand(~flagattr))

    event = {'type': 'update_message_flags',
             'operation': operation,
             'flag': flag,
             'messages': messages,
             'all': all}
    log_event(event)
    send_event(event, [user_profile.id])

    statsd.incr("flags.%s.%s" % (flag, operation), count)
    return count

def subscribed_to_stream(user_profile, stream):
    # type: (UserProfile, Stream) -> bool
    try:
        if Subscription.objects.get(user_profile=user_profile,
                                    active=True,
                                    recipient__type=Recipient.STREAM,
                                    recipient__type_id=stream.id):
            return True
        return False
    except Subscription.DoesNotExist:
        return False

def truncate_content(content, max_length, truncation_message):
    # type: (Text, int, Text) -> Text
    if len(content) > max_length:
        content = content[:max_length - len(truncation_message)] + truncation_message
    return content

def truncate_body(body):
    # type: (Text) -> Text
    return truncate_content(body, MAX_MESSAGE_LENGTH, "...")

def truncate_topic(topic):
    # type: (Text) -> Text
    return truncate_content(topic, MAX_SUBJECT_LENGTH, "...")


def update_user_message_flags(message, ums):
    # type: (Message, Iterable[UserMessage]) -> None
    wildcard = message.mentions_wildcard
    mentioned_ids = message.mentions_user_ids
    ids_with_alert_words = message.user_ids_with_alert_words
    changed_ums = set() # type: Set[UserMessage]

    def update_flag(um, should_set, flag):
        # type: (UserMessage, bool, int) -> None
        if should_set:
            if not (um.flags & flag):
                um.flags |= flag
                changed_ums.add(um)
        else:
            if (um.flags & flag):
                um.flags &= ~flag
                changed_ums.add(um)

    for um in ums:
        has_alert_word = um.user_profile_id in ids_with_alert_words
        update_flag(um, has_alert_word, UserMessage.flags.has_alert_word)

        mentioned = um.user_profile_id in mentioned_ids
        update_flag(um, mentioned, UserMessage.flags.mentioned)

        update_flag(um, wildcard, UserMessage.flags.wildcard_mentioned)

        is_me_message = getattr(message, 'is_me_message', False)
        update_flag(um, is_me_message, UserMessage.flags.is_me_message)

    for um in changed_ums:
        um.save(update_fields=['flags'])

def update_to_dict_cache(changed_messages):
    # type: (List[Message]) -> List[int]
    """Updates the message as stored in the to_dict cache (for serving
    messages)."""
    items_for_remote_cache = {}
    message_ids = []
    for changed_message in changed_messages:
        message_ids.append(changed_message.id)
        items_for_remote_cache[to_dict_cache_key(changed_message, True)] = \
            (MessageDict.to_dict_uncached(changed_message, apply_markdown=True),)
        items_for_remote_cache[to_dict_cache_key(changed_message, False)] = \
            (MessageDict.to_dict_uncached(changed_message, apply_markdown=False),)
    cache_set_many(items_for_remote_cache)
    return message_ids

# We use transaction.atomic to support select_for_update in the attachment codepath.
@transaction.atomic
def do_update_embedded_data(user_profile, message, content, rendered_content):
    # type: (UserProfile, Message, Optional[Text], Optional[Text]) -> None
    event = {
        'type': 'update_message',
        'sender': user_profile.email,
        'message_id': message.id}  # type: Dict[str, Any]
    changed_messages = [message]

    ums = UserMessage.objects.filter(message=message.id)

    if content is not None:
        update_user_message_flags(message, ums)
        message.content = content
        message.rendered_content = rendered_content
        message.rendered_content_version = bugdown_version
        event["content"] = content
        event["rendered_content"] = rendered_content

    log_event(event)
    message.save(update_fields=["content", "rendered_content"])

    event['message_ids'] = update_to_dict_cache(changed_messages)

    def user_info(um):
        # type: (UserMessage) -> Dict[str, Any]
        return {
            'id': um.user_profile_id,
            'flags': um.flags_list()
        }
    send_event(event, list(map(user_info, ums)))

# We use transaction.atomic to support select_for_update in the attachment codepath.
@transaction.atomic
def do_update_message(user_profile, message, subject, propagate_mode, content, rendered_content):
    # type: (UserProfile, Message, Optional[Text], str, Optional[Text], Optional[Text]) -> int
    event = {'type': 'update_message',
             'sender': user_profile.email,
             'message_id': message.id} # type: Dict[str, Any]
    edit_history_event = {} # type: Dict[str, Any]
    changed_messages = [message]

    # Set first_rendered_content to be the oldest version of the
    # rendered content recorded; which is the current version if the
    # content hasn't been edited before.  Note that because one could
    # have edited just the subject, not every edit history event
    # contains a prev_rendered_content element.
    first_rendered_content = message.rendered_content
    if message.edit_history is not None:
        edit_history = ujson.loads(message.edit_history)
        for old_edit_history_event in edit_history:
            if 'prev_rendered_content' in old_edit_history_event:
                first_rendered_content = old_edit_history_event['prev_rendered_content']

    ums = UserMessage.objects.filter(message=message.id)

    if content is not None:
        update_user_message_flags(message, ums)

        # We are turning off diff highlighting everywhere until ticket #1532 is addressed.
        if False:
            # Don't highlight message edit diffs on prod
            rendered_content = highlight_html_differences(first_rendered_content, rendered_content)

        event['orig_content'] = message.content
        event['orig_rendered_content'] = message.rendered_content
        edit_history_event["prev_content"] = message.content
        edit_history_event["prev_rendered_content"] = message.rendered_content
        edit_history_event["prev_rendered_content_version"] = message.rendered_content_version
        message.content = content
        message.rendered_content = rendered_content
        message.rendered_content_version = bugdown_version
        event["content"] = content
        event["rendered_content"] = rendered_content

        prev_content = edit_history_event['prev_content']
        if Message.content_has_attachment(prev_content) or Message.content_has_attachment(message.content):
            check_attachment_reference_change(prev_content, message)

    if subject is not None:
        orig_subject = message.topic_name()
        subject = truncate_topic(subject)
        event["orig_subject"] = orig_subject
        event["propagate_mode"] = propagate_mode
        message.subject = subject
        event["stream_id"] = message.recipient.type_id
        event["subject"] = subject
        event['subject_links'] = bugdown.subject_links(message.sender.realm_id, subject)
        edit_history_event["prev_subject"] = orig_subject

        if propagate_mode in ["change_later", "change_all"]:
            propagate_query = Q(recipient = message.recipient, subject = orig_subject)
            # We only change messages up to 2 days in the past, to avoid hammering our
            # DB by changing an unbounded amount of messages
            if propagate_mode == 'change_all':
                before_bound = now() - datetime.timedelta(days=2)

                propagate_query = (propagate_query & ~Q(id = message.id) &
                                   Q(pub_date__range=(before_bound, now())))
            if propagate_mode == 'change_later':
                propagate_query = propagate_query & Q(id__gt = message.id)

            messages = Message.objects.filter(propagate_query).select_related()

            # Evaluate the query before running the update
            messages_list = list(messages)
            messages.update(subject=subject)

            for m in messages_list:
                # The cached ORM object is not changed by messages.update()
                # and the remote cache update requires the new value
                m.subject = subject

            changed_messages += messages_list

    message.last_edit_time = timezone.now()
    event['edit_timestamp'] = datetime_to_timestamp(message.last_edit_time)
    edit_history_event['timestamp'] = event['edit_timestamp']
    if message.edit_history is not None:
        edit_history.insert(0, edit_history_event)
    else:
        edit_history = [edit_history_event]
    message.edit_history = ujson.dumps(edit_history)

    log_event(event)
    message.save(update_fields=["subject", "content", "rendered_content",
                                "rendered_content_version", "last_edit_time",
                                "edit_history"])

    event['message_ids'] = update_to_dict_cache(changed_messages)

    def user_info(um):
        # type: (UserMessage) -> Dict[str, Any]
        return {
            'id': um.user_profile_id,
            'flags': um.flags_list()
        }
    send_event(event, list(map(user_info, ums)))
    return len(changed_messages)

def encode_email_address(stream):
    # type: (Stream) -> Text
    return encode_email_address_helper(stream.name, stream.email_token)

def encode_email_address_helper(name, email_token):
    # type: (Text, Text) -> Text
    # Some deployments may not use the email gateway
    if settings.EMAIL_GATEWAY_PATTERN == '':
        return ''

    # Given the fact that we have almost no restrictions on stream names and
    # that what characters are allowed in e-mail addresses is complicated and
    # dependent on context in the address, we opt for a very simple scheme:
    #
    # Only encode the stream name (leave the + and token alone). Encode
    # everything that isn't alphanumeric plus _ as the percent-prefixed integer
    # ordinal of that character, padded with zeroes to the maximum number of
    # bytes of a UTF-8 encoded Unicode character.
    encoded_name = re.sub("\W", lambda x: "%" + str(ord(x.group(0))).zfill(4), name)
    encoded_token = "%s+%s" % (encoded_name, email_token)
    return settings.EMAIL_GATEWAY_PATTERN % (encoded_token,)

def get_email_gateway_message_string_from_address(address):
    # type: (Text) -> Optional[Text]
    pattern_parts = [re.escape(part) for part in settings.EMAIL_GATEWAY_PATTERN.split('%s')]
    if settings.EMAIL_GATEWAY_EXTRA_PATTERN_HACK:
        # Accept mails delivered to any Zulip server
        pattern_parts[-1] = settings.EMAIL_GATEWAY_EXTRA_PATTERN_HACK
    match_email_re = re.compile("(.*?)".join(pattern_parts))
    match = match_email_re.match(address)

    if not match:
        return None

    msg_string = match.group(1)

    return msg_string

def decode_email_address(email):
    # type: (Text) -> Tuple[Text, Text]
    # Perform the reverse of encode_email_address. Returns a tuple of (streamname, email_token)
    msg_string = get_email_gateway_message_string_from_address(email)

    if '.' in msg_string:
        # Workaround for Google Groups and other programs that don't accept emails
        # that have + signs in them (see Trac #2102)
        encoded_stream_name, token = msg_string.split('.')
    else:
        encoded_stream_name, token = msg_string.split('+')
    stream_name = re.sub("%\d{4}", lambda x: unichr(int(x.group(0)[1:])), encoded_stream_name)
    return stream_name, token

# In general, it's better to avoid using .values() because it makes
# the code pretty ugly, but in this case, it has significant
# performance impact for loading / for users with large numbers of
# subscriptions, so it's worth optimizing.
def gather_subscriptions_helper(user_profile):
    # type: (UserProfile) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]
    sub_dicts = Subscription.objects.select_related("recipient").filter(
        user_profile    = user_profile,
        recipient__type = Recipient.STREAM).values(
        "recipient__type_id", "in_home_view", "color", "desktop_notifications",
        "audible_notifications", "active", "pin_to_top")

    stream_ids = set([sub["recipient__type_id"] for sub in sub_dicts])
    all_streams = get_active_streams(user_profile.realm).select_related(
        "realm").values("id", "name", "invite_only", "realm_id",
                        "realm__domain", "email_token", "description")

    stream_dicts = [stream for stream in all_streams if stream['id'] in stream_ids]
    stream_hash = {}
    for stream in stream_dicts:
        stream_hash[stream["id"]] = stream

    all_streams_id = [stream["id"] for stream in all_streams]

    subscribed = []
    unsubscribed = []
    never_subscribed = []

    # Deactivated streams aren't in stream_hash.
    streams = [stream_hash[sub["recipient__type_id"]] for sub in sub_dicts
               if sub["recipient__type_id"] in stream_hash]
    streams_subscribed_map = dict((sub["recipient__type_id"], sub["active"]) for sub in sub_dicts)

    # Add never subscribed streams to streams_subscribed_map
    streams_subscribed_map.update({stream['id']: False for stream in all_streams if stream not in streams})

    subscriber_map = bulk_get_subscriber_user_ids(all_streams, user_profile, streams_subscribed_map)

    sub_unsub_stream_ids = set()
    for sub in sub_dicts:
        sub_unsub_stream_ids.add(sub["recipient__type_id"])
        stream = stream_hash.get(sub["recipient__type_id"])
        if not stream:
            # This stream has been deactivated, don't include it.
            continue

        subscribers = subscriber_map[stream["id"]]

        # Important: don't show the subscribers if the stream is invite only
        # and this user isn't on it anymore.
        if stream["invite_only"] and not sub["active"]:
            subscribers = None

        stream_dict = {'name': stream["name"],
                       'in_home_view': sub["in_home_view"],
                       'invite_only': stream["invite_only"],
                       'color': sub["color"],
                       'desktop_notifications': sub["desktop_notifications"],
                       'audible_notifications': sub["audible_notifications"],
                       'pin_to_top': sub["pin_to_top"],
                       'stream_id': stream["id"],
                       'description': stream["description"],
                       'email_address': encode_email_address_helper(stream["name"], stream["email_token"])}
        if subscribers is not None:
            stream_dict['subscribers'] = subscribers
        if sub["active"]:
            subscribed.append(stream_dict)
        else:
            unsubscribed.append(stream_dict)

    all_streams_id_set = set(all_streams_id)
    # Listing public streams are disabled for Zephyr mirroring realms.
    if user_profile.realm.is_zephyr_mirror_realm:
        never_subscribed_stream_ids = set() # type: Set[int]
    else:
        never_subscribed_stream_ids = all_streams_id_set - sub_unsub_stream_ids
    never_subscribed_streams = [ns_stream_dict for ns_stream_dict in all_streams
                                if ns_stream_dict['id'] in never_subscribed_stream_ids]

    for stream in never_subscribed_streams:
        if not stream['invite_only']:
            stream_dict = {'name': stream['name'],
                           'invite_only': stream['invite_only'],
                           'stream_id': stream['id'],
                           'description': stream['description']}
            subscribers = subscriber_map[stream["id"]]
            if subscribers is not None:
                stream_dict['subscribers'] = subscribers
            never_subscribed.append(stream_dict)

    return (sorted(subscribed, key=lambda x: x['name']),
            sorted(unsubscribed, key=lambda x: x['name']),
            sorted(never_subscribed, key=lambda x: x['name']))

def gather_subscriptions(user_profile):
    # type: (UserProfile) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]
    subscribed, unsubscribed, never_subscribed = gather_subscriptions_helper(user_profile)
    user_ids = set()
    for subs in [subscribed, unsubscribed, never_subscribed]:
        for sub in subs:
            if 'subscribers' in sub:
                for subscriber in sub['subscribers']:
                    user_ids.add(subscriber)
    email_dict = get_emails_from_user_ids(list(user_ids))

    for subs in [subscribed, unsubscribed]:
        for sub in subs:
            if 'subscribers' in sub:
                sub['subscribers'] = [email_dict[user_id] for user_id in sub['subscribers']]

    return (subscribed, unsubscribed)

def get_status_dict(requesting_user_profile):
    # type: (UserProfile) -> Dict[Text, Dict[Text, Dict[str, Any]]]
    if requesting_user_profile.realm.presence_disabled:
        # Return an empty dict if presence is disabled in this realm
        return defaultdict(dict)

    return UserPresence.get_status_dict_by_realm(requesting_user_profile.realm_id)


def get_realm_user_dicts(user_profile):
    # type: (UserProfile) -> List[Dict[str, Text]]
    return [{'email': userdict['email'],
             'user_id': userdict['id'],
             'is_admin': userdict['is_realm_admin'],
             'is_bot': userdict['is_bot'],
             'full_name': userdict['full_name']}
            for userdict in get_active_user_dicts_in_realm(user_profile.realm)]

def get_cross_realm_dicts():
    # type: () -> List[Dict[str, Any]]
    users = [get_user_profile_by_email(email) for email in get_cross_realm_emails()]
    return [{'email': user.email,
             'user_id': user.id,
             'is_admin': user.is_realm_admin,
             'is_bot': user.is_bot,
             'full_name': user.full_name}
            for user in users]

# Fetch initial data.  When event_types is not specified, clients want
# all event types.  Whenever you add new code to this function, you
# should also add corresponding events for changes in the data
# structures and new code to apply_events (and add a test in EventsRegisterTest).
def fetch_initial_state_data(user_profile, event_types, queue_id):
    # type: (UserProfile, Optional[Iterable[str]], str) -> Dict[str, Any]
    state = {'queue_id': queue_id} # type: Dict[str, Any]

    if event_types is None:
        want = lambda msg_type: True
    else:
        want = set(event_types).__contains__

    if want('alert_words'):
        state['alert_words'] = user_alert_words(user_profile)

    if want('message'):
        # The client should use get_old_messages() to fetch messages
        # starting with the max_message_id.  They will get messages
        # newer than that ID via get_events()
        messages = Message.objects.filter(usermessage__user_profile=user_profile).order_by('-id')[:1]
        if messages:
            state['max_message_id'] = messages[0].id
        else:
            state['max_message_id'] = -1

    if want('muted_topics'):
        state['muted_topics'] = ujson.loads(user_profile.muted_topics)

    if want('pointer'):
        state['pointer'] = user_profile.pointer

    if want('presence'):
        state['presences'] = get_status_dict(user_profile)

    if want('realm'):
        state['realm_name'] = user_profile.realm.name
        state['realm_restricted_to_domain'] = user_profile.realm.restricted_to_domain
        state['realm_invite_required'] = user_profile.realm.invite_required
        state['realm_invite_by_admins_only'] = user_profile.realm.invite_by_admins_only
        state['realm_authentication_methods'] = user_profile.realm.authentication_methods_dict()
        state['realm_create_stream_by_admins_only'] = user_profile.realm.create_stream_by_admins_only
        state['realm_add_emoji_by_admins_only'] = user_profile.realm.add_emoji_by_admins_only
        state['realm_allow_message_editing'] = user_profile.realm.allow_message_editing
        state['realm_message_content_edit_limit_seconds'] = user_profile.realm.message_content_edit_limit_seconds
        state['realm_default_language'] = user_profile.realm.default_language
        state['realm_waiting_period_threshold'] = user_profile.realm.waiting_period_threshold

    if want('realm_domain'):
        state['realm_domain'] = user_profile.realm.domain

    if want('realm_domains'):
        state['realm_domains'] = get_realm_aliases(user_profile.realm)

    if want('realm_emoji'):
        state['realm_emoji'] = user_profile.realm.get_emoji()

    if want('realm_filters'):
        state['realm_filters'] = realm_filters_for_realm(user_profile.realm_id)

    if want('realm_user'):
        state['realm_users'] = get_realm_user_dicts(user_profile)

    if want('realm_bot'):
        state['realm_bots'] = get_owned_bot_dicts(user_profile)

    if want('referral'):
        state['referrals'] = {'granted': user_profile.invites_granted,
                              'used': user_profile.invites_used}

    if want('subscription'):
        subscriptions, unsubscribed, never_subscribed = gather_subscriptions_helper(user_profile)
        state['subscriptions'] = subscriptions
        state['unsubscribed'] = unsubscribed
        state['never_subscribed'] = never_subscribed

    if want('update_message_flags'):
        # There's no initial data for message flag updates, client will
        # get any updates during a session from get_events()
        pass

    if want('stream'):
        state['streams'] = do_get_streams(user_profile)
    if want('default_streams'):
        state['realm_default_streams'] = streams_to_dicts_sorted(get_default_streams_for_realm(user_profile.realm))

    if want('update_display_settings'):
        state['twenty_four_hour_time'] = user_profile.twenty_four_hour_time
        state['left_side_userlist'] = user_profile.left_side_userlist

        default_language = user_profile.default_language
        state['default_language'] = default_language

    if want('update_global_notifications'):
        state['enable_stream_desktop_notifications'] = user_profile.enable_stream_desktop_notifications
        state['enable_stream_sounds'] = user_profile.enable_stream_sounds
        state['enable_desktop_notifications'] = user_profile.enable_desktop_notifications
        state['enable_sounds'] = user_profile.enable_sounds
        state['enable_offline_email_notifications'] = user_profile.enable_offline_email_notifications
        state['enable_offline_push_notifications'] = user_profile.enable_offline_push_notifications
        state['enable_online_push_notifications'] = user_profile.enable_online_push_notifications
        state['enable_digest_emails'] = user_profile.enable_digest_emails

    return state

def apply_events(state, events, user_profile):
    # type: (Dict[str, Any], Iterable[Dict[str, Any]], UserProfile) -> None
    for event in events:
        if event['type'] == "message":
            state['max_message_id'] = max(state['max_message_id'], event['message']['id'])
        elif event['type'] == "pointer":
            state['pointer'] = max(state['pointer'], event['pointer'])
        elif event['type'] == "realm_user":
            person = event['person']

            def our_person(p):
                # type: (Dict[str, Any]) -> bool
                return p['user_id'] == person['user_id']

            if event['op'] == "add":
                state['realm_users'].append(person)
            elif event['op'] == "remove":
                state['realm_users'] = [user for user in state['realm_users'] if not our_person(user)]
            elif event['op'] == 'update':
                for p in state['realm_users']:
                    if our_person(p):
                        # In the unlikely event that the current user
                        # just changed to/from being an admin, we need
                        # to add/remove the data on all bots in the
                        # realm.  This is ugly and probably better
                        # solved by removing the all-realm-bots data
                        # given to admin users from this flow.
                        if ('is_admin' in person and 'realm_bots' in state and
                                user_profile.email == person['email']):
                            if p['is_admin'] and not person['is_admin']:
                                state['realm_bots'] = []
                            if not p['is_admin'] and person['is_admin']:
                                state['realm_bots'] = get_owned_bot_dicts(user_profile)
                        # Now update the person
                        p.update(person)
        elif event['type'] == 'realm_bot':
            if event['op'] == 'add':
                state['realm_bots'].append(event['bot'])

            if event['op'] == 'remove':
                email = event['bot']['email']
                state['realm_bots'] = [b for b in state['realm_bots'] if b['email'] != email]

            if event['op'] == 'update':
                for bot in state['realm_bots']:
                    if bot['email'] == event['bot']['email']:
                        bot.update(event['bot'])

        elif event['type'] == 'stream':
            if event['op'] == 'create':
                for stream in event['streams']:
                    if not stream['invite_only']:
                        stream_data = copy.deepcopy(stream)
                        stream_data['subscribers'] = []
                        # Add stream to never_subscribed (if not invite_only)
                        state['never_subscribed'].append(stream_data)

            if event['op'] == 'delete':
                deleted_stream_ids = {stream['stream_id'] for stream in event['streams']}
                state['streams'] = [s for s in state['streams'] if s['stream_id'] not in deleted_stream_ids]
                state['never_subscribed'] = [stream for stream in state['never_subscribed'] if
                                             stream['stream_id'] not in deleted_stream_ids]

            if event['op'] == 'update':
                # For legacy reasons, we call stream data 'subscriptions' in
                # the state var here, for the benefit of the JS code.
                for obj in state['subscriptions']:
                    if obj['name'].lower() == event['name'].lower():
                        obj[event['property']] = event['value']
                # Also update the pure streams data
                for stream in state['streams']:
                    if stream['name'].lower() == event['name'].lower():
                        prop = event['property']
                        if prop in stream:
                            stream[prop] = event['value']
            elif event['op'] == "occupy":
                state['streams'] += event['streams']
            elif event['op'] == "vacate":
                stream_ids = [s["stream_id"] for s in event['streams']]
                state['streams'] = [s for s in state['streams'] if s["stream_id"] not in stream_ids]
        elif event['type'] == 'default_streams':
            state['realm_default_streams'] = event['default_streams']
        elif event['type'] == 'realm':
            if event['op'] == "update":
                field = 'realm_' + event['property']
                state[field] = event['value']
            elif event['op'] == "update_dict":
                for key, value in event['data'].items():
                    state['realm_' + key] = value
        elif event['type'] == "subscription":
            if event['op'] in ["add"]:
                # Convert the user_profile IDs to emails since that's what register() returns
                # TODO: Clean up this situation
                for item in event["subscriptions"]:
                    item["subscribers"] = [get_user_profile_by_email(email).id for email in item["subscribers"]]

            def name(sub):
                # type: (Dict[str, Any]) -> Text
                return sub['name'].lower()

            if event['op'] == "add":
                added_names = set(map(name, event["subscriptions"]))
                was_added = lambda s: name(s) in added_names

                # add the new subscriptions
                state['subscriptions'] += event['subscriptions']

                # remove them from unsubscribed if they had been there
                state['unsubscribed'] = [s for s in state['unsubscribed'] if not was_added(s)]

                # remove them from never_subscribed if they had been there
                state['never_subscribed'] = [s for s in state['never_subscribed'] if not was_added(s)]

            elif event['op'] == "remove":
                removed_names = set(map(name, event["subscriptions"]))
                was_removed = lambda s: name(s) in removed_names

                # Find the subs we are affecting.
                removed_subs = list(filter(was_removed, state['subscriptions']))

                # Remove our user from the subscribers of the removed subscriptions.
                for sub in removed_subs:
                    sub['subscribers'] = [id for id in sub['subscribers'] if id != user_profile.id]

                # We must effectively copy the removed subscriptions from subscriptions to
                # unsubscribe, since we only have the name in our data structure.
                state['unsubscribed'] += removed_subs

                # Now filter out the removed subscriptions from subscriptions.
                state['subscriptions'] = [s for s in state['subscriptions'] if not was_removed(s)]

            elif event['op'] == 'update':
                for sub in state['subscriptions']:
                    if sub['name'].lower() == event['name'].lower():
                        sub[event['property']] = event['value']
            elif event['op'] == 'peer_add':
                user_id = event['user_id']
                for sub in state['subscriptions']:
                    if (sub['name'] in event['subscriptions'] and
                            user_id not in sub['subscribers']):
                        sub['subscribers'].append(user_id)
                for sub in state['never_subscribed']:
                    if (sub['name'] in event['subscriptions'] and
                            user_id not in sub['subscribers']):
                        sub['subscribers'].append(user_id)
            elif event['op'] == 'peer_remove':
                user_id = event['user_id']
                for sub in state['subscriptions']:
                    if (sub['name'] in event['subscriptions'] and
                            user_id in sub['subscribers']):
                        sub['subscribers'].remove(user_id)
        elif event['type'] == "presence":
            state['presences'][event['email']] = event['presence']
        elif event['type'] == "update_message":
            # The client will get the updated message directly
            pass
        elif event['type'] == "reaction":
            # The client will get the message with the reactions directly
            pass
        elif event['type'] == "referral":
            state['referrals'] = event['referrals']
        elif event['type'] == "update_message_flags":
            # The client will get the message with the updated flags directly
            pass
        elif event['type'] == "realm_domains":
            if event['op'] == 'add':
                state['realm_domains'].append(event['alias'])
            elif event['op'] == 'remove':
                state['realm_domains'] = [alias for alias in state['realm_domains'] if alias['domain'] != event['domain']]
        elif event['type'] == "realm_emoji":
            state['realm_emoji'] = event['realm_emoji']
        elif event['type'] == "alert_words":
            state['alert_words'] = event['alert_words']
        elif event['type'] == "muted_topics":
            state['muted_topics'] = event["muted_topics"]
        elif event['type'] == "realm_filters":
            state['realm_filters'] = event["realm_filters"]
        elif event['type'] == "update_display_settings":
            if event['setting_name'] == "twenty_four_hour_time":
                state['twenty_four_hour_time'] = event["setting"]
            if event['setting_name'] == 'left_side_userlist':
                state['left_side_userlist'] = event["setting"]
        elif event['type'] == "update_global_notifications":
            if event['notification_name'] == "enable_stream_desktop_notifications":
                state['enable_stream_desktop_notifications'] = event['setting']
            elif event['notification_name'] == "enable_stream_sounds":
                state['enable_stream_sounds'] = event['setting']
            elif event['notification_name'] == "enable_desktop_notifications":
                state['enable_desktop_notifications'] = event['setting']
            elif event['notification_name'] == "enable_sounds":
                state['enable_sounds'] = event['setting']
            elif event['notification_name'] == "enable_offline_email_notifications":
                state['enable_offline_email_notifications'] = event['setting']
            elif event['notification_name'] == "enable_offline_push_notifications":
                state['enable_offline_push_notifications'] = event['setting']
            elif event['notification_name'] == "enable_online_push_notifications":
                state['enable_online_push_notifications'] = event['setting']
            elif event['notification_name'] == "enable_digest_emails":
                state['enable_digest_emails'] = event['setting']
        else:
            raise ValueError("Unexpected event type %s" % (event['type'],))

def do_events_register(user_profile, user_client, apply_markdown=True,
                       event_types=None, queue_lifespan_secs=0, all_public_streams=False,
                       narrow=[]):
    # type: (UserProfile, Client, bool, Optional[Iterable[str]], int, bool, Iterable[Sequence[Text]]) -> Dict[str, Any]
    # Technically we don't need to check this here because
    # build_narrow_filter will check it, but it's nicer from an error
    # handling perspective to do it before contacting Tornado
    check_supported_events_narrow_filter(narrow)
    queue_id = request_event_queue(user_profile, user_client, apply_markdown,
                                   queue_lifespan_secs, event_types, all_public_streams,
                                   narrow=narrow)

    if queue_id is None:
        raise JsonableError(_("Could not allocate event queue"))
    if event_types is not None:
        event_types_set = set(event_types) # type: Optional[Set[str]]
    else:
        event_types_set = None

    ret = fetch_initial_state_data(user_profile, event_types_set, queue_id)

    # Apply events that came in while we were fetching initial data
    events = get_user_events(user_profile, queue_id, -1)
    apply_events(ret, events, user_profile)
    if events:
        ret['last_event_id'] = events[-1]['id']
    else:
        ret['last_event_id'] = -1
    return ret

def do_send_confirmation_email(invitee, referrer):
    # type: (PreregistrationUser, UserProfile) -> None
    """
    Send the confirmation/welcome e-mail to an invited user.

    `invitee` is a PreregistrationUser.
    `referrer` is a UserProfile.
    """
    subject_template_path = 'confirmation/invite_email_subject.txt'
    body_template_path = 'confirmation/invite_email_body.txt'
    context = {'referrer': referrer,
               'support_email': settings.ZULIP_ADMINISTRATOR,
               'verbose_support_offers': settings.VERBOSE_SUPPORT_OFFERS}

    if referrer.realm.is_zephyr_mirror_realm:
        subject_template_path = 'confirmation/mituser_invite_email_subject.txt'
        body_template_path = 'confirmation/mituser_invite_email_body.txt'

    Confirmation.objects.send_confirmation(
        invitee, invitee.email, additional_context=context,
        subject_template_path=subject_template_path,
        body_template_path=body_template_path, host=referrer.realm.host)

@statsd_increment("push_notifications")
def handle_push_notification(user_profile_id, missed_message):
    # type: (int, Dict[str, Any]) -> None
    try:
        user_profile = get_user_profile_by_id(user_profile_id)
        if not (receives_offline_notifications(user_profile) or receives_online_notifications(user_profile)):
            return

        umessage = UserMessage.objects.get(user_profile=user_profile,
                                           message__id=missed_message['message_id'])
        message = umessage.message
        if umessage.flags.read:
            return
        sender_str = message.sender.full_name

        apple = num_push_devices_for_user(user_profile, kind=PushDeviceToken.APNS)
        android = num_push_devices_for_user(user_profile, kind=PushDeviceToken.GCM)

        if apple or android:
            # TODO: set badge count in a better way
            # Determine what alert string to display based on the missed messages
            if message.recipient.type == Recipient.HUDDLE:
                alert = "New private group message from %s" % (sender_str,)
            elif message.recipient.type == Recipient.PERSONAL:
                alert = "New private message from %s" % (sender_str,)
            elif message.recipient.type == Recipient.STREAM:
                alert = "New mention from %s" % (sender_str,)
            else:
                alert = "New Zulip mentions and private messages from %s" % (sender_str,)

            if apple:
                apple_extra_data = {'message_ids': [message.id]}
                send_apple_push_notification(user_profile, alert, badge=1, zulip=apple_extra_data)

            if android:
                content = message.content
                content_truncated = (len(content) > 200)
                if content_truncated:
                    content = content[:200] + "..."

                android_data = {
                    'user': user_profile.email,
                    'event': 'message',
                    'alert': alert,
                    'zulip_message_id': message.id, # message_id is reserved for CCS
                    'time': datetime_to_timestamp(message.pub_date),
                    'content': content,
                    'content_truncated': content_truncated,
                    'sender_email': message.sender.email,
                    'sender_full_name': message.sender.full_name,
                    'sender_avatar_url': get_avatar_url(message.sender.avatar_source, message.sender.email),
                }

                if message.recipient.type == Recipient.STREAM:
                    android_data['recipient_type'] = "stream"
                    android_data['stream'] = get_display_recipient(message.recipient)
                    android_data['topic'] = message.subject
                elif message.recipient.type in (Recipient.HUDDLE, Recipient.PERSONAL):
                    android_data['recipient_type'] = "private"

                send_android_push_notification(user_profile, android_data)

    except UserMessage.DoesNotExist:
        logging.error("Could not find UserMessage with message_id %s" % (missed_message['message_id'],))

def is_inactive(email):
    # type: (Text) -> None
    try:
        if get_user_profile_by_email(email).is_active:
            raise ValidationError(u'%s is already active' % (email,))
    except UserProfile.DoesNotExist:
        pass

def user_email_is_unique(email):
    # type: (Text) -> None
    try:
        get_user_profile_by_email(email)
        raise ValidationError(u'%s is already registered' % (email,))
    except UserProfile.DoesNotExist:
        pass

def validate_email(user_profile, email):
    # type: (UserProfile, Text) -> Tuple[Optional[str], Optional[str]]
    try:
        validators.validate_email(email)
    except ValidationError:
        return _("Invalid address."), None

    if not email_allowed_for_realm(email, user_profile.realm):
        return _("Outside your domain."), None

    try:
        existing_user_profile = get_user_profile_by_email(email)
    except UserProfile.DoesNotExist:
        existing_user_profile = None

    try:
        if existing_user_profile is not None and existing_user_profile.is_mirror_dummy:
            # Mirror dummy users to be activated must be inactive
            is_inactive(email)
        else:
            # Other users should not already exist at all.
            user_email_is_unique(email)
    except ValidationError:
        return None, _("Already has an account.")

    return None, None

def do_invite_users(user_profile, invitee_emails, streams):
    # type: (UserProfile, SizedTextIterable, Iterable[Stream]) -> Tuple[Optional[str], Dict[str, Union[List[Tuple[Text, str]], bool]]]
    validated_emails = [] # type: List[Text]
    errors = [] # type: List[Tuple[Text, str]]
    skipped = [] # type: List[Tuple[Text, str]]

    ret_error = None # type: Optional[str]
    ret_error_data = {} # type: Dict[str, Union[List[Tuple[Text, str]], bool]]

    for email in invitee_emails:
        if email == '':
            continue

        email_error, email_skipped = validate_email(user_profile, email)

        if not (email_error or email_skipped):
            validated_emails.append(email)
        elif email_error:
            errors.append((email, email_error))
        elif email_skipped:
            skipped.append((email, email_skipped))

    if errors:
        ret_error = _("Some emails did not validate, so we didn't send any invitations.")
        ret_error_data = {'errors': errors + skipped, 'sent_invitations': False}
        return ret_error, ret_error_data

    if skipped and len(skipped) == len(invitee_emails):
        # All e-mails were skipped, so we didn't actually invite anyone.
        ret_error = _("We weren't able to invite anyone.")
        ret_error_data = {'errors': skipped, 'sent_invitations': False}
        return ret_error, ret_error_data

    # Now that we are past all the possible errors, we actually create
    # the PreregistrationUser objects and trigger the email invitations.
    for email in validated_emails:
        # The logged in user is the referrer.
        prereg_user = PreregistrationUser(email=email, referred_by=user_profile)

        # We save twice because you cannot associate a ManyToMany field
        # on an unsaved object.
        prereg_user.save()
        prereg_user.streams = streams
        prereg_user.save()

        event = {"email": prereg_user.email, "referrer_email": user_profile.email}
        queue_json_publish("invites", event,
                           lambda event: do_send_confirmation_email(prereg_user, user_profile))

    if skipped:
        ret_error = _("Some of those addresses are already using Zulip, "
                      "so we didn't send them an invitation. We did send "
                      "invitations to everyone else!")
        ret_error_data = {'errors': skipped, 'sent_invitations': True}

    return ret_error, ret_error_data

def send_referral_event(user_profile):
    # type: (UserProfile) -> None
    event = dict(type="referral",
                 referrals=dict(granted=user_profile.invites_granted,
                                used=user_profile.invites_used))
    send_event(event, [user_profile.id])

def do_refer_friend(user_profile, email):
    # type: (UserProfile, Text) -> None
    content = ('Referrer: "%s" <%s>\n'
               'Realm: %s\n'
               'Referred: %s') % (user_profile.full_name, user_profile.email,
                                  user_profile.realm.domain, email)
    subject = "Zulip referral: %s" % (email,)
    from_email = '"%s" <%s>' % (user_profile.full_name, 'referrals@zulip.com')
    to_email = '"Zulip Referrals" <zulip+referrals@zulip.com>'
    headers = {'Reply-To': '"%s" <%s>' % (user_profile.full_name, user_profile.email,)}
    msg = EmailMessage(subject, content, from_email, [to_email], headers=headers)
    msg.send()

    referral = Referral(user_profile=user_profile, email=email)
    referral.save()
    user_profile.invites_used += 1
    user_profile.save(update_fields=['invites_used'])

    send_referral_event(user_profile)

def notify_realm_emoji(realm):
    # type: (Realm) -> None
    event = dict(type="realm_emoji", op="update",
                 realm_emoji=realm.get_emoji())
    user_ids = [userdict['id'] for userdict in get_active_user_dicts_in_realm(realm)]
    send_event(event, user_ids)

def check_add_realm_emoji(realm, name, img_url, author=None):
    # type: (Realm, Text, Text, Optional[UserProfile]) -> None
    emoji = RealmEmoji(realm=realm, name=name, img_url=img_url, author=author)
    emoji.full_clean()
    emoji.save()
    notify_realm_emoji(realm)

def do_remove_realm_emoji(realm, name):
    # type: (Realm, Text) -> None
    RealmEmoji.objects.get(realm=realm, name=name).delete()
    notify_realm_emoji(realm)

def notify_alert_words(user_profile, words):
    # type: (UserProfile, Iterable[Text]) -> None
    event = dict(type="alert_words", alert_words=words)
    send_event(event, [user_profile.id])

def do_add_alert_words(user_profile, alert_words):
    # type: (UserProfile, Iterable[Text]) -> None
    words = add_user_alert_words(user_profile, alert_words)
    notify_alert_words(user_profile, words)

def do_remove_alert_words(user_profile, alert_words):
    # type: (UserProfile, Iterable[Text]) -> None
    words = remove_user_alert_words(user_profile, alert_words)
    notify_alert_words(user_profile, words)

def do_set_alert_words(user_profile, alert_words):
    # type: (UserProfile, List[Text]) -> None
    set_user_alert_words(user_profile, alert_words)
    notify_alert_words(user_profile, alert_words)

def do_set_muted_topics(user_profile, muted_topics):
    # type: (UserProfile, Union[List[List[Text]], List[Tuple[Text, Text]]]) -> None
    user_profile.muted_topics = ujson.dumps(muted_topics)
    user_profile.save(update_fields=['muted_topics'])
    event = dict(type="muted_topics", muted_topics=muted_topics)
    send_event(event, [user_profile.id])

def notify_realm_filters(realm):
    # type: (Realm) -> None
    realm_filters = realm_filters_for_realm(realm.id)
    user_ids = [userdict['id'] for userdict in get_active_user_dicts_in_realm(realm)]
    event = dict(type="realm_filters", realm_filters=realm_filters)
    send_event(event, user_ids)

# NOTE: Regexes must be simple enough that they can be easily translated to JavaScript
# RegExp syntax. In addition to JS-compatible syntax, the following features are available:
#   * Named groups will be converted to numbered groups automatically
#   * Inline-regex flags will be stripped, and where possible translated to RegExp-wide flags
def do_add_realm_filter(realm, pattern, url_format_string):
    # type: (Realm, Text, Text) -> int
    pattern = pattern.strip()
    url_format_string = url_format_string.strip()
    realm_filter = RealmFilter(
        realm=realm, pattern=pattern,
        url_format_string=url_format_string)
    realm_filter.full_clean()
    realm_filter.save()
    notify_realm_filters(realm)

    return realm_filter.id

def do_remove_realm_filter(realm, pattern=None, id=None):
    # type: (Realm, Optional[Text], Optional[int]) -> None
    if pattern is not None:
        RealmFilter.objects.get(realm=realm, pattern=pattern).delete()
    else:
        RealmFilter.objects.get(realm=realm, pk=id).delete()
    notify_realm_filters(realm)

def get_emails_from_user_ids(user_ids):
    # type: (Sequence[int]) -> Dict[int, Text]
    # We may eventually use memcached to speed this up, but the DB is fast.
    return UserProfile.emails_from_ids(user_ids)

def get_realm_aliases(realm):
    # type: (Realm) -> List[Dict[str, Text]]
    return list(realm.realmalias_set.values('domain'))

def do_add_realm_alias(realm, domain):
    # type: (Realm, Text) -> (RealmAlias)
    alias = RealmAlias.objects.create(realm=realm, domain=domain)
    event = dict(type="realm_domains", op="add",
                 alias=dict(domain=alias.domain,
                            ))
    send_event(event, active_user_ids(realm))
    return alias

def do_remove_realm_alias(realm, domain):
    # type: (Realm, Text) -> None
    RealmAlias.objects.get(realm=realm, domain=domain).delete()
    event = dict(type="realm_domains", op="remove", domain=domain)
    send_event(event, active_user_ids(realm))

def get_occupied_streams(realm):
    # type: (Realm) -> QuerySet
    # TODO: Make a generic stub for QuerySet
    """ Get streams with subscribers """
    subs_filter = Subscription.objects.filter(active=True, user_profile__realm=realm,
                                              user_profile__is_active=True).values('recipient_id')
    stream_ids = Recipient.objects.filter(
        type=Recipient.STREAM, id__in=subs_filter).values('type_id')

    return Stream.objects.filter(id__in=stream_ids, realm=realm, deactivated=False)

def do_get_streams(user_profile, include_public=True, include_subscribed=True,
                   include_all_active=False, include_default=False):
    # type: (UserProfile, bool, bool, bool, bool) -> List[Dict[str, Any]]
    if include_all_active and not user_profile.is_api_super_user:
        raise JsonableError(_("User not authorized for this query"))

    # Listing public streams are disabled for Zephyr mirroring realms.
    include_public = include_public and not user_profile.realm.is_zephyr_mirror_realm
    # Start out with all streams in the realm with subscribers
    query = get_occupied_streams(user_profile.realm)

    if not include_all_active:
        user_subs = Subscription.objects.select_related("recipient").filter(
            active=True, user_profile=user_profile,
            recipient__type=Recipient.STREAM)

        if include_subscribed:
            recipient_check = Q(id__in=[sub.recipient.type_id for sub in user_subs])
        if include_public:
            invite_only_check = Q(invite_only=False)

        if include_subscribed and include_public:
            query = query.filter(recipient_check | invite_only_check)
        elif include_public:
            query = query.filter(invite_only_check)
        elif include_subscribed:
            query = query.filter(recipient_check)
        else:
            # We're including nothing, so don't bother hitting the DB.
            query = []

    streams = [(row.to_dict()) for row in query]
    streams.sort(key=lambda elt: elt["name"])
    if include_default:
        is_default = {}
        default_streams = get_default_streams_for_realm(user_profile.realm)
        for default_stream in default_streams:
            is_default[default_stream.id] = True
        for stream in streams:
            stream['is_default'] = is_default.get(stream["stream_id"], False)

    return streams

def do_claim_attachments(message):
    # type: (Message) -> List[Tuple[Text, bool]]
    attachment_url_list = attachment_url_re.findall(message.content)

    results = []
    for url in attachment_url_list:
        path_id = attachment_url_to_path_id(url)
        user_profile = message.sender
        is_message_realm_public = False
        if message.recipient.type == Recipient.STREAM:
            is_message_realm_public = Stream.objects.get(id=message.recipient.type_id).is_public()

        if path_id is not None:
            is_claimed = claim_attachment(user_profile, path_id, message,
                                          is_message_realm_public)
            results.append((path_id, is_claimed))

    return results

def do_delete_old_unclaimed_attachments(weeks_ago):
    # type: (int) -> None
    old_unclaimed_attachments = get_old_unclaimed_attachments(weeks_ago)

    for attachment in old_unclaimed_attachments:
        delete_message_image(attachment.path_id)
        attachment.delete()

def check_attachment_reference_change(prev_content, message):
    # type: (Text, Message) -> None
    new_content = message.content
    prev_attachments = set(attachment_url_re.findall(prev_content))
    new_attachments = set(attachment_url_re.findall(new_content))

    to_remove = list(prev_attachments - new_attachments)
    path_ids = []
    for url in to_remove:
        path_id = attachment_url_to_path_id(url)
        path_ids.append(path_id)

    attachments_to_update = Attachment.objects.filter(path_id__in=path_ids).select_for_update()
    message.attachment_set.remove(*attachments_to_update)

    to_add = list(new_attachments - prev_attachments)
    if len(to_add) > 0:
        do_claim_attachments(message)
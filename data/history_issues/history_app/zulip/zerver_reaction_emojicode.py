import datetime
import itertools
import logging
import os
import platform
import time
from collections import defaultdict
from operator import itemgetter
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import django.db.utils
import ujson
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.core.files import File
from django.db import IntegrityError, connection, transaction
from django.db.models import Count, Exists, F, Max, OuterRef, Q, Sum
from django.db.models.query import QuerySet
from django.utils.html import escape
from django.utils.timezone import now as timezone_now
from django.utils.translation import ugettext as _
from psycopg2.extras import execute_values
from psycopg2.sql import SQL
from typing_extensions import TypedDict

from analytics.lib.counts import COUNT_STATS, RealmCount, do_increment_logging_stat
from analytics.models import StreamCount
from confirmation import settings as confirmation_settings
from confirmation.models import (
    Confirmation,
    confirmation_url,
    create_confirmation_link,
    generate_key,
)
from zerver.decorator import statsd_increment
from zerver.lib import bugdown
from zerver.lib.addressee import Addressee
from zerver.lib.alert_words import (
    add_user_alert_words,
    get_alert_word_automaton,
    remove_user_alert_words,
)
from zerver.lib.avatar import avatar_url, avatar_url_from_dict
from zerver.lib.bot_config import ConfigError, get_bot_config, get_bot_configs, set_bot_config
from zerver.lib.bugdown import version as bugdown_version
from zerver.lib.bulk_create import bulk_create_users
from zerver.lib.cache import (
    bot_dict_fields,
    cache_delete,
    cache_delete_many,
    cache_set,
    cache_set_many,
    cache_with_key,
    delete_user_profile_caches,
    display_recipient_cache_key,
    flush_user_profile,
    to_dict_cache_key_id,
    user_profile_by_api_key_cache_key,
    user_profile_by_email_cache_key,
)
from zerver.lib.context_managers import lockfile
from zerver.lib.create_user import create_user, get_display_email_address
from zerver.lib.email_mirror_helpers import encode_email_address, encode_email_address_helper
from zerver.lib.email_notifications import enqueue_welcome_emails
from zerver.lib.email_validation import (
    email_reserved_for_system_bots_error,
    get_existing_user_errors,
    get_realm_email_validator,
    validate_email_is_valid,
)
from zerver.lib.emoji import emoji_name_to_emoji_code, get_emoji_file_name
from zerver.lib.exceptions import (
    BugdownRenderingException,
    ErrorCode,
    JsonableError,
    StreamDoesNotExistError,
    StreamWithIDDoesNotExistError,
)
from zerver.lib.export import get_realm_exports_serialized
from zerver.lib.external_accounts import DEFAULT_EXTERNAL_ACCOUNTS
from zerver.lib.hotspots import get_next_hotspots
from zerver.lib.i18n import get_language_name
from zerver.lib.message import (
    MessageDict,
    access_message,
    render_markdown,
    truncate_body,
    truncate_topic,
    update_first_visible_message_id,
)
from zerver.lib.pysa import mark_sanitized
from zerver.lib.queue import queue_json_publish
from zerver.lib.realm_icon import realm_icon_url
from zerver.lib.realm_logo import get_realm_logo_data
from zerver.lib.retention import move_messages_to_archive
from zerver.lib.send_email import (
    FromAddress,
    clear_scheduled_emails,
    clear_scheduled_invitation_emails,
    send_email,
    send_email_to_admins,
)
from zerver.lib.server_initialization import create_internal_realm, server_initialized
from zerver.lib.sessions import delete_user_sessions
from zerver.lib.storage import static_path
from zerver.lib.stream_recipient import StreamRecipientMap
from zerver.lib.stream_subscription import (
    get_active_subscriptions_for_stream_id,
    get_active_subscriptions_for_stream_ids,
    get_bulk_stream_subscriber_info,
    get_stream_subscriptions_for_user,
    get_stream_subscriptions_for_users,
    get_subscribed_stream_ids_for_user,
    num_subscribers_for_stream_id,
)
from zerver.lib.stream_topic import StreamTopicTarget
from zerver.lib.streams import (
    access_stream_for_send_message,
    check_stream_name,
    create_stream_if_needed,
    get_default_value_for_history_public_to_subscribers,
    render_stream_description,
    send_stream_creation_event,
    subscribed_to_stream,
)
from zerver.lib.timestamp import datetime_to_timestamp, timestamp_to_datetime
from zerver.lib.topic import (
    LEGACY_PREV_TOPIC,
    ORIG_TOPIC,
    TOPIC_LINKS,
    TOPIC_NAME,
    filter_by_exact_message_topic,
    filter_by_topic_name_via_message,
    save_message_for_edit_use_case,
    update_messages_for_topic_edit,
)
from zerver.lib.topic_mutes import add_topic_mute, get_topic_mutes, remove_topic_mute
from zerver.lib.types import ProfileFieldData
from zerver.lib.upload import (
    claim_attachment,
    delete_avatar_image,
    delete_export_tarball,
    delete_message_image,
    upload_emoji_image,
)
from zerver.lib.user_groups import access_user_group_by_id, create_user_group
from zerver.lib.user_status import update_user_status
from zerver.lib.users import (
    check_bot_name_available,
    check_full_name,
    format_user_row,
    get_api_key,
    user_profile_to_user_row,
)
from zerver.lib.utils import generate_api_key, log_statsd_event
from zerver.lib.validator import check_widget_content
from zerver.lib.widget import do_widget_post_save_actions
from zerver.models import (
    MAX_MESSAGE_LENGTH,
    Attachment,
    Client,
    CustomProfileField,
    CustomProfileFieldValue,
    DefaultStream,
    DefaultStreamGroup,
    EmailChangeStatus,
    Message,
    MultiuseInvite,
    PreregistrationUser,
    Reaction,
    Realm,
    RealmAuditLog,
    RealmDomain,
    RealmEmoji,
    RealmFilter,
    Recipient,
    ScheduledEmail,
    ScheduledMessage,
    Service,
    Stream,
    SubMessage,
    Subscription,
    UserActivity,
    UserActivityInterval,
    UserGroup,
    UserGroupMembership,
    UserHotspot,
    UserMessage,
    UserPresence,
    UserProfile,
    UserStatus,
    active_non_guest_user_ids,
    active_user_ids,
    custom_profile_fields_for_realm,
    email_to_username,
    filter_to_valid_prereg_users,
    get_active_streams,
    get_bot_dicts_in_realm,
    get_bot_services,
    get_client,
    get_default_stream_groups,
    get_huddle_recipient,
    get_huddle_user_ids,
    get_old_unclaimed_attachments,
    get_stream,
    get_stream_by_id_in_realm,
    get_stream_cache_key,
    get_system_bot,
    get_user_by_delivery_email,
    get_user_by_id_in_realm_including_cross_realm,
    get_user_profile_by_id,
    is_cross_realm_bot_email,
    query_for_ids,
    realm_filters_for_realm,
    stream_name_in_use,
    validate_attachment_request,
)
from zerver.tornado.event_queue import send_event

if settings.BILLING_ENABLED:
    from corporate.lib.stripe import downgrade_now, update_license_ledger_if_needed

# This will be used to type annotate parameters in a function if the function
# works on both str and unicode in python 2 but in python 3 it only works on str.
SizedTextIterable = Union[Sequence[str], AbstractSet[str]]
ONBOARDING_TOTAL_MESSAGES = 1000
ONBOARDING_UNREAD_MESSAGES = 20

STREAM_ASSIGNMENT_COLORS = [
    "#76ce90", "#fae589", "#a6c7e5", "#e79ab5",
    "#bfd56f", "#f4ae55", "#b0a5fd", "#addfe5",
    "#f5ce6e", "#c2726a", "#94c849", "#bd86e5",
    "#ee7e4a", "#a6dcbf", "#95a5fd", "#53a063",
    "#9987e1", "#e4523d", "#c2c2c2", "#4f8de4",
    "#c6a8ad", "#e7cc4d", "#c8bebf", "#a47462"]

def subscriber_info(user_id: int) -> Dict[str, Any]:
    return {
        'id': user_id,
        'flags': ['read']
    }

# Store an event in the log for re-importing messages
def log_event(event: MutableMapping[str, Any]) -> None:
    if settings.EVENT_LOG_DIR is None:
        return

    if "timestamp" not in event:
        event["timestamp"] = time.time()

    if not os.path.exists(settings.EVENT_LOG_DIR):
        os.mkdir(settings.EVENT_LOG_DIR)

    template = os.path.join(settings.EVENT_LOG_DIR,
                            '%s.' + platform.node() +
                            timezone_now().strftime('.%Y-%m-%d'))

    with lockfile(template % ('lock',)):
        with open(template % ('events',), 'a') as log:
            log.write(ujson.dumps(event) + '\n')

def can_access_stream_user_ids(stream: Stream) -> Set[int]:
    # return user ids of users who can access the attributes of
    # a stream, such as its name/description.
    if stream.is_public():
        # For a public stream, this is everyone in the realm
        # except unsubscribed guest users
        return public_stream_user_ids(stream)
    else:
        # for a private stream, it's subscribers plus realm admins.
        return private_stream_user_ids(
            stream.id) | {user.id for user in stream.realm.get_admin_users_and_bots()}

def private_stream_user_ids(stream_id: int) -> Set[int]:
    # TODO: Find similar queries elsewhere and de-duplicate this code.
    subscriptions = get_active_subscriptions_for_stream_id(stream_id)
    return {sub['user_profile_id'] for sub in subscriptions.values('user_profile_id')}

def public_stream_user_ids(stream: Stream) -> Set[int]:
    guest_subscriptions = get_active_subscriptions_for_stream_id(
        stream.id).filter(user_profile__role=UserProfile.ROLE_GUEST)
    guest_subscriptions = {sub['user_profile_id'] for sub in guest_subscriptions.values('user_profile_id')}
    return set(active_non_guest_user_ids(stream.realm_id)) | guest_subscriptions

def bot_owner_user_ids(user_profile: UserProfile) -> Set[int]:
    is_private_bot = (
        user_profile.default_sending_stream and
        user_profile.default_sending_stream.invite_only or
        user_profile.default_events_register_stream and
        user_profile.default_events_register_stream.invite_only)
    if is_private_bot:
        return {user_profile.bot_owner_id}
    else:
        users = {user.id for user in user_profile.realm.get_human_admin_users()}
        users.add(user_profile.bot_owner_id)
        return users

def realm_user_count(realm: Realm) -> int:
    return UserProfile.objects.filter(realm=realm, is_active=True, is_bot=False).count()

def realm_user_count_by_role(realm: Realm) -> Dict[str, Any]:
    human_counts = {UserProfile.ROLE_REALM_ADMINISTRATOR: 0,
                    UserProfile.ROLE_REALM_OWNER: 0,
                    UserProfile.ROLE_MEMBER: 0,
                    UserProfile.ROLE_GUEST: 0}
    for value_dict in list(UserProfile.objects.filter(
            realm=realm, is_bot=False, is_active=True).values('role').annotate(Count('role'))):
        human_counts[value_dict['role']] = value_dict['role__count']
    bot_count = UserProfile.objects.filter(realm=realm, is_bot=True, is_active=True).count()
    return {
        RealmAuditLog.ROLE_COUNT_HUMANS: human_counts,
        RealmAuditLog.ROLE_COUNT_BOTS: bot_count,
    }

def get_signups_stream(realm: Realm) -> Stream:
    # This one-liner helps us work around a lint rule.
    return get_stream("signups", realm)

def notify_new_user(user_profile: UserProfile) -> None:
    sender_email = settings.NOTIFICATION_BOT
    sender = get_system_bot(sender_email)

    user_count = realm_user_count(user_profile.realm)
    signup_notifications_stream = user_profile.realm.get_signup_notifications_stream()
    # Send notification to realm signup notifications stream if it exists
    # Don't send notification for the first user in a realm
    if signup_notifications_stream is not None and user_count > 1:
        internal_send_stream_message(
            user_profile.realm,
            sender,
            signup_notifications_stream,
            "signups",
            f"@_**{user_profile.full_name}|{user_profile.id}** just signed up for Zulip. (total: {user_count})",
        )

    # We also send a notification to the Zulip administrative realm
    admin_realm = sender.realm
    try:
        # Check whether the stream exists
        signups_stream = get_signups_stream(admin_realm)
        internal_send_stream_message(
            admin_realm,
            sender,
            signups_stream,
            user_profile.realm.display_subdomain,
            f"{user_profile.full_name} <`{user_profile.email}`> just signed up for Zulip! (total: **{user_count}**)",
        )

    except Stream.DoesNotExist:
        # If the signups stream hasn't been created in the admin
        # realm, don't auto-create it to send to it; just do nothing.
        pass

def notify_invites_changed(user_profile: UserProfile) -> None:
    event = dict(type="invites_changed")
    admin_ids = [user.id for user in
                 user_profile.realm.get_admin_users_and_bots()]
    send_event(user_profile.realm, event, admin_ids)

def add_new_user_history(user_profile: UserProfile, streams: Iterable[Stream]) -> None:
    """Give you the last ONBOARDING_TOTAL_MESSAGES messages on your public
    streams, so you have something to look at in your home view once
    you finish the tutorial.  The most recent ONBOARDING_UNREAD_MESSAGES
    are marked unread.
    """
    one_week_ago = timezone_now() - datetime.timedelta(weeks=1)

    recipient_ids = [stream.recipient_id for stream in streams if not stream.invite_only]
    recent_messages = Message.objects.filter(recipient_id__in=recipient_ids,
                                             date_sent__gt=one_week_ago).order_by("-id")
    message_ids_to_use = list(reversed(recent_messages.values_list(
        'id', flat=True)[0:ONBOARDING_TOTAL_MESSAGES]))
    if len(message_ids_to_use) == 0:
        return

    # Handle the race condition where a message arrives between
    # bulk_add_subscriptions above and the Message query just above
    already_ids = set(UserMessage.objects.filter(message_id__in=message_ids_to_use,
                                                 user_profile=user_profile).values_list("message_id",
                                                                                        flat=True))

    # Mark the newest ONBOARDING_UNREAD_MESSAGES as unread.
    marked_unread = 0
    ums_to_create = []
    for message_id in reversed(message_ids_to_use):
        if message_id in already_ids:
            continue

        um = UserMessage(user_profile=user_profile, message_id=message_id)
        if marked_unread < ONBOARDING_UNREAD_MESSAGES:
            marked_unread += 1
        else:
            um.flags = UserMessage.flags.read
        ums_to_create.append(um)

    UserMessage.objects.bulk_create(reversed(ums_to_create))

# Does the processing for a new user account:
# * Subscribes to default/invitation streams
# * Fills in some recent historical messages
# * Notifies other users in realm and Zulip about the signup
# * Deactivates PreregistrationUser objects
# * subscribe the user to newsletter if newsletter_data is specified
def process_new_human_user(user_profile: UserProfile,
                           prereg_user: Optional[PreregistrationUser]=None,
                           newsletter_data: Optional[Mapping[str, str]]=None,
                           default_stream_groups: Sequence[DefaultStreamGroup]=[],
                           realm_creation: bool=False) -> None:
    mit_beta_user = user_profile.realm.is_zephyr_mirror_realm
    if prereg_user is not None:
        prereg_user.status = confirmation_settings.STATUS_ACTIVE
        prereg_user.save(update_fields=['status'])
        streams = prereg_user.streams.all()
        acting_user: Optional[UserProfile] = prereg_user.referred_by
    else:
        streams = []
        acting_user = None

    # If the user's invitation didn't explicitly list some streams, we
    # add the default streams
    if len(streams) == 0:
        streams = get_default_subs(user_profile)

    for default_stream_group in default_stream_groups:
        default_stream_group_streams = default_stream_group.streams.all()
        for stream in default_stream_group_streams:
            if stream not in streams:
                streams.append(stream)

    bulk_add_subscriptions(streams, [user_profile], acting_user=acting_user)

    add_new_user_history(user_profile, streams)

    # mit_beta_users don't have a referred_by field
    if not mit_beta_user and prereg_user is not None and prereg_user.referred_by is not None:
        # This is a cross-realm private message.
        internal_send_private_message(
            user_profile.realm,
            get_system_bot(settings.NOTIFICATION_BOT),
            prereg_user.referred_by,
            f"{user_profile.full_name} <`{user_profile.email}`> accepted your invitation to join Zulip!",
        )
    # Mark any other PreregistrationUsers that are STATUS_ACTIVE as
    # inactive so we can keep track of the PreregistrationUser we
    # actually used for analytics
    if prereg_user is not None:
        PreregistrationUser.objects.filter(
            email__iexact=user_profile.delivery_email).exclude(id=prereg_user.id)\
            .update(status=confirmation_settings.STATUS_REVOKED)

        if prereg_user.referred_by is not None:
            notify_invites_changed(user_profile)
    else:
        PreregistrationUser.objects.filter(email__iexact=user_profile.delivery_email)\
            .update(status=confirmation_settings.STATUS_REVOKED)

    notify_new_user(user_profile)
    # Clear any scheduled invitation emails to prevent them
    # from being sent after the user is created.
    clear_scheduled_invitation_emails(user_profile.delivery_email)
    if user_profile.realm.send_welcome_emails:
        enqueue_welcome_emails(user_profile, realm_creation)

    # We have an import loop here; it's intentional, because we want
    # to keep all the onboarding code in zerver/lib/onboarding.py.
    from zerver.lib.onboarding import send_initial_pms
    send_initial_pms(user_profile)

    if newsletter_data is not None:
        # If the user was created automatically via the API, we may
        # not want to register them for the newsletter
        queue_json_publish(
            "signups",
            {
                'email_address': user_profile.delivery_email,
                'user_id': user_profile.id,
                'merge_fields': {
                    'NAME': user_profile.full_name,
                    'REALM_ID': user_profile.realm_id,
                    'OPTIN_IP': newsletter_data["IP"],
                    'OPTIN_TIME': datetime.datetime.isoformat(timezone_now().replace(microsecond=0)),
                },
            },
            lambda event: None)

def notify_created_user(user_profile: UserProfile) -> None:
    user_row = user_profile_to_user_row(user_profile)
    person = format_user_row(user_profile.realm, user_profile, user_row,
                             # Since we don't know what the client
                             # supports at this point in the code, we
                             # just assume client_gravatar and
                             # user_avatar_url_field_optional = False :(
                             client_gravatar=False,
                             user_avatar_url_field_optional=False,
                             # We assume there's no custom profile
                             # field data for a new user; initial
                             # values are expected to be added in a
                             # later event.
                             custom_profile_field_data={})
    event: Dict[str, Any] = dict(type="realm_user", op="add", person=person)
    send_event(user_profile.realm, event, active_user_ids(user_profile.realm_id))

def created_bot_event(user_profile: UserProfile) -> Dict[str, Any]:
    def stream_name(stream: Optional[Stream]) -> Optional[str]:
        if not stream:
            return None
        return stream.name

    default_sending_stream_name = stream_name(user_profile.default_sending_stream)
    default_events_register_stream_name = stream_name(user_profile.default_events_register_stream)

    bot = dict(email=user_profile.email,
               user_id=user_profile.id,
               full_name=user_profile.full_name,
               bot_type=user_profile.bot_type,
               is_active=user_profile.is_active,
               api_key=get_api_key(user_profile),
               default_sending_stream=default_sending_stream_name,
               default_events_register_stream=default_events_register_stream_name,
               default_all_public_streams=user_profile.default_all_public_streams,
               avatar_url=avatar_url(user_profile),
               services = get_service_dicts_for_bot(user_profile.id),
               )

    # Set the owner key only when the bot has an owner.
    # The default bots don't have an owner. So don't
    # set the owner key while reactivating them.
    if user_profile.bot_owner is not None:
        bot['owner_id'] = user_profile.bot_owner.id

    return dict(type="realm_bot", op="add", bot=bot)

def notify_created_bot(user_profile: UserProfile) -> None:
    event = created_bot_event(user_profile)
    send_event(user_profile.realm, event, bot_owner_user_ids(user_profile))

def create_users(realm: Realm, name_list: Iterable[Tuple[str, str]], bot_type: Optional[int]=None) -> None:
    user_set = set()
    for full_name, email in name_list:
        short_name = email_to_username(email)
        user_set.add((email, full_name, short_name, True))
    bulk_create_users(realm, user_set, bot_type)

def do_create_user(email: str, password: Optional[str], realm: Realm, full_name: str,
                   short_name: str, bot_type: Optional[int]=None, role: Optional[int]=None,
                   bot_owner: Optional[UserProfile]=None, tos_version: Optional[str]=None,
                   timezone: str="", avatar_source: str=UserProfile.AVATAR_FROM_GRAVATAR,
                   default_sending_stream: Optional[Stream]=None,
                   default_events_register_stream: Optional[Stream]=None,
                   default_all_public_streams: Optional[bool]=None,
                   prereg_user: Optional[PreregistrationUser]=None,
                   newsletter_data: Optional[Dict[str, str]]=None,
                   default_stream_groups: Sequence[DefaultStreamGroup]=[],
                   source_profile: Optional[UserProfile]=None,
                   realm_creation: bool=False) -> UserProfile:

    user_profile = create_user(email=email, password=password, realm=realm,
                               full_name=full_name, short_name=short_name,
                               role=role, bot_type=bot_type, bot_owner=bot_owner,
                               tos_version=tos_version, timezone=timezone, avatar_source=avatar_source,
                               default_sending_stream=default_sending_stream,
                               default_events_register_stream=default_events_register_stream,
                               default_all_public_streams=default_all_public_streams,
                               source_profile=source_profile)

    event_time = user_profile.date_joined
    RealmAuditLog.objects.create(
        realm=user_profile.realm, modified_user=user_profile,
        event_type=RealmAuditLog.USER_CREATED, event_time=event_time,
        extra_data=ujson.dumps({
            RealmAuditLog.ROLE_COUNT: realm_user_count_by_role(user_profile.realm),
        }))
    do_increment_logging_stat(user_profile.realm, COUNT_STATS['active_users_log:is_bot:day'],
                              user_profile.is_bot, event_time)
    if settings.BILLING_ENABLED:
        update_license_ledger_if_needed(user_profile.realm, event_time)

    # Note that for bots, the caller will send an additional event
    # with bot-specific info like services.
    notify_created_user(user_profile)
    if bot_type is None:
        process_new_human_user(user_profile, prereg_user=prereg_user,
                               newsletter_data=newsletter_data,
                               default_stream_groups=default_stream_groups,
                               realm_creation=realm_creation)
    return user_profile

def do_activate_user(user_profile: UserProfile) -> None:
    user_profile.is_active = True
    user_profile.is_mirror_dummy = False
    user_profile.set_unusable_password()
    user_profile.date_joined = timezone_now()
    user_profile.tos_version = settings.TOS_VERSION
    user_profile.save(update_fields=["is_active", "date_joined", "password",
                                     "is_mirror_dummy", "tos_version"])

    event_time = user_profile.date_joined
    RealmAuditLog.objects.create(
        realm=user_profile.realm, modified_user=user_profile,
        event_type=RealmAuditLog.USER_ACTIVATED, event_time=event_time,
        extra_data=ujson.dumps({
            RealmAuditLog.ROLE_COUNT: realm_user_count_by_role(user_profile.realm),
        }))
    do_increment_logging_stat(user_profile.realm, COUNT_STATS['active_users_log:is_bot:day'],
                              user_profile.is_bot, event_time)
    if settings.BILLING_ENABLED:
        update_license_ledger_if_needed(user_profile.realm, event_time)

    notify_created_user(user_profile)

def do_reactivate_user(user_profile: UserProfile, acting_user: Optional[UserProfile]=None) -> None:
    # Unlike do_activate_user, this is meant for re-activating existing users,
    # so it doesn't reset their password, etc.
    user_profile.is_active = True
    user_profile.save(update_fields=["is_active"])

    event_time = timezone_now()
    RealmAuditLog.objects.create(
        realm=user_profile.realm, modified_user=user_profile, acting_user=acting_user,
        event_type=RealmAuditLog.USER_REACTIVATED, event_time=event_time,
        extra_data=ujson.dumps({
            RealmAuditLog.ROLE_COUNT: realm_user_count_by_role(user_profile.realm),
        }))
    do_increment_logging_stat(user_profile.realm, COUNT_STATS['active_users_log:is_bot:day'],
                              user_profile.is_bot, event_time)
    if settings.BILLING_ENABLED:
        update_license_ledger_if_needed(user_profile.realm, event_time)

    notify_created_user(user_profile)

    if user_profile.is_bot:
        notify_created_bot(user_profile)

def active_humans_in_realm(realm: Realm) -> Sequence[UserProfile]:
    return UserProfile.objects.filter(realm=realm, is_active=True, is_bot=False)


def do_set_realm_property(realm: Realm, name: str, value: Any) -> None:
    """Takes in a realm object, the name of an attribute to update, and the
    value to update.
    """
    property_type = Realm.property_types[name]
    assert isinstance(value, property_type), (
        f'Cannot update {name}: {value} is not an instance of {property_type}')

    old_value = getattr(realm, name)
    setattr(realm, name, value)
    realm.save(update_fields=[name])

    event = dict(
        type='realm',
        op='update',
        property=name,
        value=value,
    )
    send_event(realm, event, active_user_ids(realm.id))

    if name == "email_address_visibility":
        if Realm.EMAIL_ADDRESS_VISIBILITY_EVERYONE not in [old_value, value]:
            # We use real email addresses on UserProfile.email only if
            # EMAIL_ADDRESS_VISIBILITY_EVERYONE is configured, so
            # changes between values that will not require changing
            # that field, so we can save work and return here.
            return

        user_profiles = UserProfile.objects.filter(realm=realm, is_bot=False)
        for user_profile in user_profiles:
            user_profile.email = get_display_email_address(user_profile, realm)
            # TODO: Design a bulk event for this or force-reload all clients
            send_user_email_update_event(user_profile)
        UserProfile.objects.bulk_update(user_profiles, ['email'])

        for user_profile in user_profiles:
            flush_user_profile(sender=UserProfile, instance=user_profile)

def do_set_realm_authentication_methods(realm: Realm,
                                        authentication_methods: Dict[str, bool]) -> None:
    for key, value in list(authentication_methods.items()):
        index = getattr(realm.authentication_methods, key).number
        realm.authentication_methods.set_bit(index, int(value))
    realm.save(update_fields=['authentication_methods'])
    event = dict(
        type="realm",
        op="update_dict",
        property='default',
        data=dict(authentication_methods=realm.authentication_methods_dict()),
    )
    send_event(realm, event, active_user_ids(realm.id))

def do_set_realm_message_editing(realm: Realm,
                                 allow_message_editing: bool,
                                 message_content_edit_limit_seconds: int,
                                 allow_community_topic_editing: bool) -> None:
    realm.allow_message_editing = allow_message_editing
    realm.message_content_edit_limit_seconds = message_content_edit_limit_seconds
    realm.allow_community_topic_editing = allow_community_topic_editing
    realm.save(update_fields=['allow_message_editing',
                              'allow_community_topic_editing',
                              'message_content_edit_limit_seconds',
                              ],
               )
    event = dict(
        type="realm",
        op="update_dict",
        property="default",
        data=dict(allow_message_editing=allow_message_editing,
                  message_content_edit_limit_seconds=message_content_edit_limit_seconds,
                  allow_community_topic_editing=allow_community_topic_editing),
    )
    send_event(realm, event, active_user_ids(realm.id))

def do_set_realm_message_deleting(realm: Realm,
                                  message_content_delete_limit_seconds: int) -> None:
    realm.message_content_delete_limit_seconds = message_content_delete_limit_seconds
    realm.save(update_fields=['message_content_delete_limit_seconds'])
    event = dict(
        type="realm",
        op="update_dict",
        property="default",
        data=dict(message_content_delete_limit_seconds=message_content_delete_limit_seconds),
    )
    send_event(realm, event, active_user_ids(realm.id))

def do_set_realm_notifications_stream(realm: Realm, stream: Stream, stream_id: int) -> None:
    realm.notifications_stream = stream
    realm.save(update_fields=['notifications_stream'])
    event = dict(
        type="realm",
        op="update",
        property="notifications_stream_id",
        value=stream_id,
    )
    send_event(realm, event, active_user_ids(realm.id))

def do_set_realm_signup_notifications_stream(realm: Realm, stream: Stream,
                                             stream_id: int) -> None:
    realm.signup_notifications_stream = stream
    realm.save(update_fields=['signup_notifications_stream'])
    event = dict(
        type="realm",
        op="update",
        property="signup_notifications_stream_id",
        value=stream_id,
    )
    send_event(realm, event, active_user_ids(realm.id))

def do_deactivate_realm(realm: Realm, acting_user: Optional[UserProfile]=None) -> None:
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

    if settings.BILLING_ENABLED:
        downgrade_now(realm)

    event_time = timezone_now()
    RealmAuditLog.objects.create(
        realm=realm, event_type=RealmAuditLog.REALM_DEACTIVATED, event_time=event_time,
        acting_user=acting_user, extra_data=ujson.dumps({
            RealmAuditLog.ROLE_COUNT: realm_user_count_by_role(realm),
        }))

    ScheduledEmail.objects.filter(realm=realm).delete()
    for user in active_humans_in_realm(realm):
        # Don't deactivate the users, but do delete their sessions so they get
        # bumped to the login screen, where they'll get a realm deactivation
        # notice when they try to log in.
        delete_user_sessions(user)

    event = dict(type="realm", op="deactivated",
                 realm_id=realm.id)
    send_event(realm, event, active_user_ids(realm.id))

def do_reactivate_realm(realm: Realm) -> None:
    realm.deactivated = False
    realm.save(update_fields=["deactivated"])

    event_time = timezone_now()
    RealmAuditLog.objects.create(
        realm=realm, event_type=RealmAuditLog.REALM_REACTIVATED, event_time=event_time,
        extra_data=ujson.dumps({
            RealmAuditLog.ROLE_COUNT: realm_user_count_by_role(realm),
        }))

def do_change_realm_subdomain(realm: Realm, new_subdomain: str) -> None:
    realm.string_id = new_subdomain
    realm.save(update_fields=["string_id"])

def do_scrub_realm(realm: Realm) -> None:
    users = UserProfile.objects.filter(realm=realm)
    for user in users:
        do_delete_messages_by_sender(user)
        do_delete_avatar_image(user)
        user.full_name = f"Scrubbed {generate_key()[:15]}"
        scrubbed_email = f"scrubbed-{generate_key()[:15]}@{realm.host}"
        user.email = scrubbed_email
        user.delivery_email = scrubbed_email
        user.save(update_fields=["full_name", "email", "delivery_email"])

    do_remove_realm_custom_profile_fields(realm)
    Attachment.objects.filter(realm=realm).delete()

    RealmAuditLog.objects.create(realm=realm, event_time=timezone_now(),
                                 event_type=RealmAuditLog.REALM_SCRUBBED)

def do_deactivate_user(user_profile: UserProfile,
                       acting_user: Optional[UserProfile]=None,
                       _cascade: bool=True) -> None:
    if not user_profile.is_active:
        return

    if user_profile.realm.is_zephyr_mirror_realm:  # nocoverage
        # For zephyr mirror users, we need to make them a mirror dummy
        # again; otherwise, other users won't get the correct behavior
        # when trying to send messages to this person inside Zulip.
        #
        # Ideally, we need to also ensure their zephyr mirroring bot
        # isn't running, but that's a separate issue.
        user_profile.is_mirror_dummy = True
    user_profile.is_active = False
    user_profile.save(update_fields=["is_active"])

    delete_user_sessions(user_profile)
    clear_scheduled_emails([user_profile.id])

    event_time = timezone_now()
    RealmAuditLog.objects.create(
        realm=user_profile.realm, modified_user=user_profile, acting_user=acting_user,
        event_type=RealmAuditLog.USER_DEACTIVATED, event_time=event_time,
        extra_data=ujson.dumps({
            RealmAuditLog.ROLE_COUNT: realm_user_count_by_role(user_profile.realm),
        }))
    do_increment_logging_stat(user_profile.realm, COUNT_STATS['active_users_log:is_bot:day'],
                              user_profile.is_bot, event_time, increment=-1)
    if settings.BILLING_ENABLED:
        update_license_ledger_if_needed(user_profile.realm, event_time)

    event = dict(type="realm_user", op="remove",
                 person=dict(user_id=user_profile.id,
                             full_name=user_profile.full_name))
    send_event(user_profile.realm, event, active_user_ids(user_profile.realm_id))

    if user_profile.is_bot:
        event = dict(type="realm_bot", op="remove",
                     bot=dict(user_id=user_profile.id,
                              full_name=user_profile.full_name))
        send_event(user_profile.realm, event, bot_owner_user_ids(user_profile))

    if _cascade:
        bot_profiles = UserProfile.objects.filter(is_bot=True, is_active=True,
                                                  bot_owner=user_profile)
        for profile in bot_profiles:
            do_deactivate_user(profile, acting_user=acting_user, _cascade=False)

def do_deactivate_stream(stream: Stream, log: bool=True) -> None:

    # Get the affected user ids *before* we deactivate everybody.
    affected_user_ids = can_access_stream_user_ids(stream)

    get_active_subscriptions_for_stream_id(stream.id).update(active=False)

    was_invite_only = stream.invite_only
    stream.deactivated = True
    stream.invite_only = True
    # Preserve as much as possible the original stream name while giving it a
    # special prefix that both indicates that the stream is deactivated and
    # frees up the original name for reuse.
    old_name = stream.name
    new_name = ("!DEACTIVATED:" + old_name)[:Stream.MAX_NAME_LENGTH]
    for i in range(20):
        if stream_name_in_use(new_name, stream.realm_id):
            # This stream has already been deactivated, keep prepending !s until
            # we have a unique stream name or you've hit a rename limit.
            new_name = ("!" + new_name)[:Stream.MAX_NAME_LENGTH]
        else:
            break

    # If you don't have a unique name at this point, this will fail later in the
    # code path.

    stream.name = new_name[:Stream.MAX_NAME_LENGTH]
    stream.save(update_fields=['name', 'deactivated', 'invite_only'])

    # If this is a default stream, remove it, properly sending a
    # notification to browser clients.
    if DefaultStream.objects.filter(realm_id=stream.realm_id, stream_id=stream.id).exists():
        do_remove_default_stream(stream)

    default_stream_groups_for_stream = DefaultStreamGroup.objects.filter(streams__id=stream.id)
    for group in default_stream_groups_for_stream:
        do_remove_streams_from_default_stream_group(stream.realm, group, [stream])

    # Remove the old stream information from remote cache.
    old_cache_key = get_stream_cache_key(old_name, stream.realm_id)
    cache_delete(old_cache_key)

    stream_dict = stream.to_dict()
    stream_dict.update(dict(name=old_name, invite_only=was_invite_only))
    event = dict(type="stream", op="delete",
                 streams=[stream_dict])
    send_event(stream.realm, event, affected_user_ids)

def send_user_email_update_event(user_profile: UserProfile) -> None:
    payload = dict(user_id=user_profile.id,
                   new_email=user_profile.email)
    send_event(user_profile.realm,
               dict(type='realm_user', op='update', person=payload),
               active_user_ids(user_profile.realm_id))

def do_change_user_delivery_email(user_profile: UserProfile, new_email: str) -> None:
    delete_user_profile_caches([user_profile])

    user_profile.delivery_email = new_email
    if user_profile.email_address_is_realm_public():
        user_profile.email = new_email
        user_profile.save(update_fields=["email", "delivery_email"])
    else:
        user_profile.save(update_fields=["delivery_email"])

    # We notify just the target user (and eventually org admins, only
    # when email_address_visibility=EMAIL_ADDRESS_VISIBILITY_ADMINS)
    # about their new delivery email, since that field is private.
    payload = dict(user_id=user_profile.id,
                   delivery_email=new_email)
    event = dict(type='realm_user', op='update', person=payload)
    send_event(user_profile.realm, event, [user_profile.id])

    if user_profile.avatar_source == UserProfile.AVATAR_FROM_GRAVATAR:
        # If the user is using Gravatar to manage their email address,
        # their Gravatar just changed, and we need to notify other
        # clients.
        notify_avatar_url_change(user_profile)

    if user_profile.email_address_is_realm_public():
        # Additionally, if we're also changing the publicly visible
        # email, we send a new_email event as well.
        send_user_email_update_event(user_profile)

    event_time = timezone_now()
    RealmAuditLog.objects.create(realm=user_profile.realm, acting_user=user_profile,
                                 modified_user=user_profile, event_type=RealmAuditLog.USER_EMAIL_CHANGED,
                                 event_time=event_time)

def do_start_email_change_process(user_profile: UserProfile, new_email: str) -> None:
    old_email = user_profile.delivery_email
    obj = EmailChangeStatus.objects.create(new_email=new_email, old_email=old_email,
                                           user_profile=user_profile, realm=user_profile.realm)

    activation_url = create_confirmation_link(obj, Confirmation.EMAIL_CHANGE)
    from zerver.context_processors import common_context
    context = common_context(user_profile)
    context.update({
        'old_email': old_email,
        'new_email': new_email,
        'activate_url': activation_url,
    })
    language = user_profile.default_language
    send_email('zerver/emails/confirm_new_email', to_emails=[new_email],
               from_name=FromAddress.security_email_from_name(language=language),
               from_address=FromAddress.tokenized_no_reply_address(),
               language=language, context=context)

def compute_irc_user_fullname(email: str) -> str:
    return email.split("@")[0] + " (IRC)"

def compute_jabber_user_fullname(email: str) -> str:
    return email.split("@")[0] + " (XMPP)"

@cache_with_key(lambda realm, email, f: user_profile_by_email_cache_key(email),
                timeout=3600*24*7)
def create_mirror_user_if_needed(realm: Realm, email: str,
                                 email_to_fullname: Callable[[str], str]) -> UserProfile:
    try:
        return get_user_by_delivery_email(email, realm)
    except UserProfile.DoesNotExist:
        try:
            # Forge a user for this person
            return create_user(
                email=email,
                password=None,
                realm=realm,
                full_name=email_to_fullname(email),
                short_name=email_to_username(email),
                active=False,
                is_mirror_dummy=True,
            )
        except IntegrityError:
            return get_user_by_delivery_email(email, realm)

def send_welcome_bot_response(message: MutableMapping[str, Any]) -> None:
    welcome_bot = get_system_bot(settings.WELCOME_BOT)
    human_recipient_id = message['message'].sender.recipient_id
    if Message.objects.filter(sender=welcome_bot, recipient_id=human_recipient_id).count() < 2:
        content = (
            _("Congratulations on your first reply!") +
            " "
            ":tada:"
            "\n"
            "\n" +
            _("Feel free to continue using this space to practice your new messaging "
              "skills. Or, try clicking on some of the stream names to your left!")
        )
        internal_send_private_message(
            message['realm'], welcome_bot, message['message'].sender, content)

def render_incoming_message(message: Message,
                            content: str,
                            user_ids: Set[int],
                            realm: Realm,
                            mention_data: Optional[bugdown.MentionData]=None,
                            email_gateway: bool=False) -> str:
    realm_alert_words_automaton = get_alert_word_automaton(realm)
    try:
        rendered_content = render_markdown(
            message=message,
            content=content,
            realm=realm,
            realm_alert_words_automaton = realm_alert_words_automaton,
            mention_data=mention_data,
            email_gateway=email_gateway,
        )
    except BugdownRenderingException:
        raise JsonableError(_('Unable to render message'))
    return rendered_content

class RecipientInfoResult(TypedDict):
    active_user_ids: Set[int]
    push_notify_user_ids: Set[int]
    stream_email_user_ids: Set[int]
    stream_push_user_ids: Set[int]
    wildcard_mention_user_ids: Set[int]
    um_eligible_user_ids: Set[int]
    long_term_idle_user_ids: Set[int]
    default_bot_user_ids: Set[int]
    service_bot_tuples: List[Tuple[int, int]]

def get_recipient_info(recipient: Recipient,
                       sender_id: int,
                       stream_topic: Optional[StreamTopicTarget],
                       possibly_mentioned_user_ids: AbstractSet[int]=set(),
                       possible_wildcard_mention: bool=True) -> RecipientInfoResult:
    stream_push_user_ids: Set[int] = set()
    stream_email_user_ids: Set[int] = set()
    wildcard_mention_user_ids: Set[int] = set()

    if recipient.type == Recipient.PERSONAL:
        # The sender and recipient may be the same id, so
        # de-duplicate using a set.
        message_to_user_ids = list({recipient.type_id, sender_id})
        assert(len(message_to_user_ids) in [1, 2])

    elif recipient.type == Recipient.STREAM:
        # Anybody calling us w/r/t a stream message needs to supply
        # stream_topic.  We may eventually want to have different versions
        # of this function for different message types.
        assert(stream_topic is not None)
        user_ids_muting_topic = stream_topic.user_ids_muting_topic()

        subscription_rows = stream_topic.get_active_subscriptions().annotate(
            user_profile_email_notifications=F('user_profile__enable_stream_email_notifications'),
            user_profile_push_notifications=F('user_profile__enable_stream_push_notifications'),
            user_profile_wildcard_mentions_notify=F(
                'user_profile__wildcard_mentions_notify'),
        ).values(
            'user_profile_id',
            'push_notifications',
            'email_notifications',
            'wildcard_mentions_notify',
            'user_profile_email_notifications',
            'user_profile_push_notifications',
            'user_profile_wildcard_mentions_notify',
            'is_muted',
        ).order_by('user_profile_id')

        message_to_user_ids = [
            row['user_profile_id']
            for row in subscription_rows
        ]

        def should_send(setting: str, row: Dict[str, Any]) -> bool:
            # This implements the structure that the UserProfile stream notification settings
            # are defaults, which can be overridden by the stream-level settings (if those
            # values are not null).
            if row['is_muted']:
                return False
            if row['user_profile_id'] in user_ids_muting_topic:
                return False
            if row[setting] is not None:
                return row[setting]
            return row['user_profile_' + setting]

        stream_push_user_ids = {
            row['user_profile_id']
            for row in subscription_rows
            # Note: muting a stream overrides stream_push_notify
            if should_send('push_notifications', row)
        }

        stream_email_user_ids = {
            row['user_profile_id']
            for row in subscription_rows
            # Note: muting a stream overrides stream_email_notify
            if should_send('email_notifications', row)
        }

        if possible_wildcard_mention:
            # If there's a possible wildcard mention, we need to
            # determine which users would receive a wildcard mention
            # notification for this message should the message indeed
            # contain a wildcard mention.
            #
            # We don't have separate values for push/email
            # notifications here; at this stage, we're just
            # determining whether this wildcard mention should be
            # treated as a mention (and follow the user's mention
            # notification preferences) or a normal message.
            wildcard_mention_user_ids = {
                row['user_profile_id']
                for row in subscription_rows
                if should_send("wildcard_mentions_notify", row)
            }

    elif recipient.type == Recipient.HUDDLE:
        message_to_user_ids = get_huddle_user_ids(recipient)

    else:
        raise ValueError('Bad recipient type')

    message_to_user_id_set = set(message_to_user_ids)

    user_ids = set(message_to_user_id_set)
    # Important note: Because we haven't rendered bugdown yet, we
    # don't yet know which of these possibly-mentioned users was
    # actually mentioned in the message (in other words, the
    # mention syntax might have been in a code block or otherwise
    # escaped).  `get_ids_for` will filter these extra user rows
    # for our data structures not related to bots
    user_ids |= possibly_mentioned_user_ids

    if user_ids:
        query = UserProfile.objects.filter(
            is_active=True,
        ).values(
            'id',
            'enable_online_push_notifications',
            'is_bot',
            'bot_type',
            'long_term_idle',
        )

        # query_for_ids is fast highly optimized for large queries, and we
        # need this codepath to be fast (it's part of sending messages)
        query = query_for_ids(
            query=query,
            user_ids=sorted(list(user_ids)),
            field='id',
        )
        rows = list(query)
    else:
        # TODO: We should always have at least one user_id as a recipient
        #       of any message we send.  Right now the exception to this
        #       rule is `notify_new_user`, which, at least in a possibly
        #       contrived test scenario, can attempt to send messages
        #       to an inactive bot.  When we plug that hole, we can avoid
        #       this `else` clause and just `assert(user_ids)`.
        #
        # UPDATE: It's February 2020 (and a couple years after the above
        #         comment was written).  We have simplified notify_new_user
        #         so that it should be a little easier to reason about.
        #         There is currently some cleanup to how we handle cross
        #         realm bots that is still under development.  Once that
        #         effort is complete, we should be able to address this
        #         to-do.
        rows = []

    def get_ids_for(f: Callable[[Dict[str, Any]], bool]) -> Set[int]:
        """Only includes users on the explicit message to line"""
        return {
            row['id']
            for row in rows
            if f(row)
        } & message_to_user_id_set

    def is_service_bot(row: Dict[str, Any]) -> bool:
        return row['is_bot'] and (row['bot_type'] in UserProfile.SERVICE_BOT_TYPES)

    active_user_ids = get_ids_for(lambda r: True)
    push_notify_user_ids = get_ids_for(
        lambda r: r['enable_online_push_notifications'],
    )

    # Service bots don't get UserMessage rows.
    um_eligible_user_ids = get_ids_for(
        lambda r: not is_service_bot(r),
    )

    long_term_idle_user_ids = get_ids_for(
        lambda r: r['long_term_idle'],
    )

    # These two bot data structures need to filter from the full set
    # of users who either are receiving the message or might have been
    # mentioned in it, and so can't use get_ids_for.
    #
    # Further in the do_send_messages code path, once
    # `mentioned_user_ids` has been computed via bugdown, we'll filter
    # these data structures for just those users who are either a
    # direct recipient or were mentioned; for now, we're just making
    # sure we have the data we need for that without extra database
    # queries.
    default_bot_user_ids = {
        row['id']
        for row in rows
        if row['is_bot'] and row['bot_type'] == UserProfile.DEFAULT_BOT
    }

    service_bot_tuples = [
        (row['id'], row['bot_type'])
        for row in rows
        if is_service_bot(row)
    ]

    info: RecipientInfoResult = dict(
        active_user_ids=active_user_ids,
        push_notify_user_ids=push_notify_user_ids,
        stream_push_user_ids=stream_push_user_ids,
        stream_email_user_ids=stream_email_user_ids,
        wildcard_mention_user_ids=wildcard_mention_user_ids,
        um_eligible_user_ids=um_eligible_user_ids,
        long_term_idle_user_ids=long_term_idle_user_ids,
        default_bot_user_ids=default_bot_user_ids,
        service_bot_tuples=service_bot_tuples,
    )
    return info

def get_service_bot_events(sender: UserProfile, service_bot_tuples: List[Tuple[int, int]],
                           mentioned_user_ids: Set[int], active_user_ids: Set[int],
                           recipient_type: int) -> Dict[str, List[Dict[str, Any]]]:

    event_dict: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # Avoid infinite loops by preventing messages sent by bots from generating
    # Service events.
    if sender.is_bot:
        return event_dict

    def maybe_add_event(user_profile_id: int, bot_type: int) -> None:
        if bot_type == UserProfile.OUTGOING_WEBHOOK_BOT:
            queue_name = 'outgoing_webhooks'
        elif bot_type == UserProfile.EMBEDDED_BOT:
            queue_name = 'embedded_bots'
        else:
            logging.error(
                'Unexpected bot_type for Service bot id=%s: %s',
                user_profile_id, bot_type,
            )
            return

        is_stream = (recipient_type == Recipient.STREAM)

        # Important note: service_bot_tuples may contain service bots
        # who were not actually mentioned in the message (e.g. if
        # mention syntax for that bot appeared in a code block).
        # Thus, it is important to filter any users who aren't part of
        # either mentioned_user_ids (the actual mentioned users) or
        # active_user_ids (the actual recipients).
        #
        # So even though this is implied by the logic below, we filter
        # these not-actually-mentioned users here, to help keep this
        # function future-proof.
        if user_profile_id not in mentioned_user_ids and user_profile_id not in active_user_ids:
            return

        # Mention triggers, for stream messages
        if is_stream and user_profile_id in mentioned_user_ids:
            trigger = 'mention'
        # PM triggers for personal and huddle messages
        elif (not is_stream) and (user_profile_id in active_user_ids):
            trigger = 'private_message'
        else:
            return

        event_dict[queue_name].append({
            'trigger': trigger,
            'user_profile_id': user_profile_id,
        })

    for user_profile_id, bot_type in service_bot_tuples:
        maybe_add_event(
            user_profile_id=user_profile_id,
            bot_type=bot_type,
        )

    return event_dict

def do_schedule_messages(messages: Sequence[Mapping[str, Any]]) -> List[int]:
    scheduled_messages: List[ScheduledMessage] = []

    for message in messages:
        scheduled_message = ScheduledMessage()
        scheduled_message.sender = message['message'].sender
        scheduled_message.recipient = message['message'].recipient
        topic_name = message['message'].topic_name()
        scheduled_message.set_topic_name(topic_name=topic_name)
        scheduled_message.content = message['message'].content
        scheduled_message.sending_client = message['message'].sending_client
        scheduled_message.stream = message['stream']
        scheduled_message.realm = message['realm']
        scheduled_message.scheduled_timestamp = message['deliver_at']
        if message['delivery_type'] == 'send_later':
            scheduled_message.delivery_type = ScheduledMessage.SEND_LATER
        elif message['delivery_type'] == 'remind':
            scheduled_message.delivery_type = ScheduledMessage.REMIND

        scheduled_messages.append(scheduled_message)

    ScheduledMessage.objects.bulk_create(scheduled_messages)
    return [scheduled_message.id for scheduled_message in scheduled_messages]


def do_send_messages(messages_maybe_none: Sequence[Optional[MutableMapping[str, Any]]],
                     email_gateway: bool=False,
                     mark_as_read: Sequence[int]=[]) -> List[int]:
    """See
    https://zulip.readthedocs.io/en/latest/subsystems/sending-messages.html
    for high-level documentation on this subsystem.
    """

    # Filter out messages which didn't pass internal_prep_message properly
    messages = [message for message in messages_maybe_none if message is not None]

    # Filter out zephyr mirror anomalies where the message was already sent
    already_sent_ids: List[int] = []
    new_messages: List[MutableMapping[str, Any]] = []
    for message in messages:
        if isinstance(message['message'], int):
            already_sent_ids.append(message['message'])
        else:
            new_messages.append(message)
    messages = new_messages

    links_for_embed: Set[str] = set()
    # For consistency, changes to the default values for these gets should also be applied
    # to the default args in do_send_message
    for message in messages:
        message['rendered_content'] = message.get('rendered_content', None)
        message['stream'] = message.get('stream', None)
        message['local_id'] = message.get('local_id', None)
        message['sender_queue_id'] = message.get('sender_queue_id', None)
        message['realm'] = message.get('realm', message['message'].sender.realm)

        mention_data = bugdown.MentionData(
            realm_id=message['realm'].id,
            content=message['message'].content,
        )
        message['mention_data'] = mention_data

        if message['message'].is_stream_message():
            stream_id = message['message'].recipient.type_id
            stream_topic: Optional[StreamTopicTarget] = StreamTopicTarget(
                stream_id=stream_id,
                topic_name=message['message'].topic_name(),
            )
        else:
            stream_topic = None

        info = get_recipient_info(
            recipient=message['message'].recipient,
            sender_id=message['message'].sender_id,
            stream_topic=stream_topic,
            possibly_mentioned_user_ids=mention_data.get_user_ids(),
            possible_wildcard_mention=mention_data.message_has_wildcards(),
        )

        message['active_user_ids'] = info['active_user_ids']
        message['push_notify_user_ids'] = info['push_notify_user_ids']
        message['stream_push_user_ids'] = info['stream_push_user_ids']
        message['stream_email_user_ids'] = info['stream_email_user_ids']
        message['um_eligible_user_ids'] = info['um_eligible_user_ids']
        message['long_term_idle_user_ids'] = info['long_term_idle_user_ids']
        message['default_bot_user_ids'] = info['default_bot_user_ids']
        message['service_bot_tuples'] = info['service_bot_tuples']

        # Render our messages.
        assert message['message'].rendered_content is None

        rendered_content = render_incoming_message(
            message['message'],
            message['message'].content,
            message['active_user_ids'],
            message['realm'],
            mention_data=message['mention_data'],
            email_gateway=email_gateway,
        )
        message['message'].rendered_content = rendered_content
        message['message'].rendered_content_version = bugdown_version
        links_for_embed |= message['message'].links_for_preview

        # Add members of the mentioned user groups into `mentions_user_ids`.
        for group_id in message['message'].mentions_user_group_ids:
            members = message['mention_data'].get_group_members(group_id)
            message['message'].mentions_user_ids.update(members)

        # Only send data to Tornado about wildcard mentions if message
        # rendering determined the message had an actual wildcard
        # mention in it (and not e.g. wildcard mention syntax inside a
        # code block).
        if message['message'].mentions_wildcard:
            message['wildcard_mention_user_ids'] = info['wildcard_mention_user_ids']
        else:
            message['wildcard_mention_user_ids'] = []

        '''
        Once we have the actual list of mentioned ids from message
        rendering, we can patch in "default bots" (aka normal bots)
        who were directly mentioned in this message as eligible to
        get UserMessage rows.
        '''
        mentioned_user_ids = message['message'].mentions_user_ids
        default_bot_user_ids = message['default_bot_user_ids']
        mentioned_bot_user_ids = default_bot_user_ids & mentioned_user_ids
        message['um_eligible_user_ids'] |= mentioned_bot_user_ids

    # Save the message receipts in the database
    user_message_flags: Dict[int, Dict[int, List[str]]] = defaultdict(dict)
    with transaction.atomic():
        Message.objects.bulk_create([message['message'] for message in messages])

        # Claim attachments in message
        for message in messages:
            if do_claim_attachments(message['message'],
                                    message['message'].potential_attachment_path_ids):
                message['message'].has_attachment = True
                message['message'].save(update_fields=['has_attachment'])

        ums: List[UserMessageLite] = []
        for message in messages:
            # Service bots (outgoing webhook bots and embedded bots) don't store UserMessage rows;
            # they will be processed later.
            mentioned_user_ids = message['message'].mentions_user_ids
            user_messages = create_user_messages(
                message=message['message'],
                um_eligible_user_ids=message['um_eligible_user_ids'],
                long_term_idle_user_ids=message['long_term_idle_user_ids'],
                stream_push_user_ids = message['stream_push_user_ids'],
                stream_email_user_ids = message['stream_email_user_ids'],
                mentioned_user_ids=mentioned_user_ids,
                mark_as_read=mark_as_read,
            )

            for um in user_messages:
                user_message_flags[message['message'].id][um.user_profile_id] = um.flags_list()

            ums.extend(user_messages)

            message['message'].service_queue_events = get_service_bot_events(
                sender=message['message'].sender,
                service_bot_tuples=message['service_bot_tuples'],
                mentioned_user_ids=mentioned_user_ids,
                active_user_ids=message['active_user_ids'],
                recipient_type=message['message'].recipient.type,
            )

        bulk_insert_ums(ums)

        for message in messages:
            do_widget_post_save_actions(message)

    for message in messages:
        realm_id: Optional[int] = None
        if message['message'].is_stream_message():
            if message['stream'] is None:
                stream_id = message['message'].recipient.type_id
                message['stream'] = Stream.objects.select_related().get(id=stream_id)
            assert message['stream'] is not None  # assert needed because stubs for django are missing
            realm_id = message['stream'].realm_id

        # Deliver events to the real-time push system, as well as
        # enqueuing any additional processing triggered by the message.
        wide_message_dict = MessageDict.wide_dict(message['message'], realm_id)

        user_flags = user_message_flags.get(message['message'].id, {})
        sender = message['message'].sender
        message_type = wide_message_dict['type']

        presence_idle_user_ids = get_active_presence_idle_user_ids(
            realm=sender.realm,
            sender_id=sender.id,
            message_type=message_type,
            active_user_ids=message['active_user_ids'],
            user_flags=user_flags,
        )

        event = dict(
            type='message',
            message=message['message'].id,
            message_dict=wide_message_dict,
            presence_idle_user_ids=presence_idle_user_ids,
        )

        '''
        TODO:  We may want to limit user_ids to only those users who have
               UserMessage rows, if only for minor performance reasons.

               For now we queue events for all subscribers/sendees of the
               message, since downstream code may still do notifications
               that don't require UserMessage rows.

               Our automated tests have gotten better on this codepath,
               but we may have coverage gaps, so we should be careful
               about changing the next line.
        '''
        user_ids = message['active_user_ids'] | set(user_flags.keys())

        users = [
            dict(
                id=user_id,
                flags=user_flags.get(user_id, []),
                always_push_notify=(user_id in message['push_notify_user_ids']),
                stream_push_notify=(user_id in message['stream_push_user_ids']),
                stream_email_notify=(user_id in message['stream_email_user_ids']),
                wildcard_mention_notify=(user_id in message['wildcard_mention_user_ids']),
            )
            for user_id in user_ids
        ]

        if message['message'].is_stream_message():
            # Note: This is where authorization for single-stream
            # get_updates happens! We only attach stream data to the
            # notify new_message request if it's a public stream,
            # ensuring that in the tornado server, non-public stream
            # messages are only associated to their subscribed users.
            assert message['stream'] is not None  # assert needed because stubs for django are missing
            if message['stream'].is_public():
                event['realm_id'] = message['stream'].realm_id
                event['stream_name'] = message['stream'].name
            if message['stream'].invite_only:
                event['invite_only'] = True
            if message['stream'].first_message_id is None:
                message['stream'].first_message_id = message['message'].id
                message['stream'].save(update_fields=["first_message_id"])
        if message['local_id'] is not None:
            event['local_id'] = message['local_id']
        if message['sender_queue_id'] is not None:
            event['sender_queue_id'] = message['sender_queue_id']
        send_event(message['realm'], event, users)

        if links_for_embed:
            event_data = {
                'message_id': message['message'].id,
                'message_content': message['message'].content,
                'message_realm_id': message['realm'].id,
                'urls': links_for_embed}
            queue_json_publish('embed_links', event_data)

        if message['message'].recipient.type == Recipient.PERSONAL:
            welcome_bot_id = get_system_bot(settings.WELCOME_BOT).id
            if (welcome_bot_id in message['active_user_ids'] and
                    welcome_bot_id != message['message'].sender_id):
                send_welcome_bot_response(message)

        for queue_name, events in message['message'].service_queue_events.items():
            for event in events:
                queue_json_publish(
                    queue_name,
                    {
                        "message": wide_message_dict,
                        "trigger": event['trigger'],
                        "user_profile_id": event["user_profile_id"],
                    },
                )

    # Note that this does not preserve the order of message ids
    # returned.  In practice, this shouldn't matter, as we only
    # mirror single zephyr messages at a time and don't otherwise
    # intermingle sending zephyr messages with other messages.
    return already_sent_ids + [message['message'].id for message in messages]

class UserMessageLite:
    '''
    The Django ORM is too slow for bulk operations.  This class
    is optimized for the simple use case of inserting a bunch of
    rows into zerver_usermessage.
    '''

    def __init__(self, user_profile_id: int, message_id: int, flags: int) -> None:
        self.user_profile_id = user_profile_id
        self.message_id = message_id
        self.flags = flags

    def flags_list(self) -> List[str]:
        return UserMessage.flags_list_for_flags(self.flags)

def create_user_messages(message: Message,
                         um_eligible_user_ids: AbstractSet[int],
                         long_term_idle_user_ids: AbstractSet[int],
                         stream_push_user_ids: AbstractSet[int],
                         stream_email_user_ids: AbstractSet[int],
                         mentioned_user_ids: AbstractSet[int],
                         mark_as_read: Sequence[int] = []) -> List[UserMessageLite]:
    ums_to_create = []
    for user_profile_id in um_eligible_user_ids:
        um = UserMessageLite(
            user_profile_id=user_profile_id,
            message_id=message.id,
            flags=0,
        )
        ums_to_create.append(um)

    # These properties on the Message are set via
    # render_markdown by code in the bugdown inline patterns
    wildcard = message.mentions_wildcard
    ids_with_alert_words = message.user_ids_with_alert_words

    for um in ums_to_create:
        if (um.user_profile_id == message.sender.id and
                message.sent_by_human()) or \
           um.user_profile_id in mark_as_read:
            um.flags |= UserMessage.flags.read
        if wildcard:
            um.flags |= UserMessage.flags.wildcard_mentioned
        if um.user_profile_id in mentioned_user_ids:
            um.flags |= UserMessage.flags.mentioned
        if um.user_profile_id in ids_with_alert_words:
            um.flags |= UserMessage.flags.has_alert_word
        if message.recipient.type in [Recipient.HUDDLE, Recipient.PERSONAL]:
            um.flags |= UserMessage.flags.is_private

    # For long_term_idle (aka soft-deactivated) users, we are allowed
    # to optimize by lazily not creating UserMessage rows that would
    # have the default 0 flag set (since the soft-reactivation logic
    # knows how to create those when the user comes back).  We need to
    # create the UserMessage rows for these long_term_idle users
    # non-lazily in a few cases:
    #
    # * There are nonzero flags (e.g. the user was mentioned), since
    #   that case is rare and this saves a lot of complexity in
    #   soft-reactivation.
    #
    # * If the user is going to be notified (e.g. they get push/email
    #   notifications for every message on a stream), since in that
    #   case the notifications code will call `access_message` on the
    #   message to re-verify permissions, and for private streams,
    #   will get an error if the UserMessage row doesn't exist yet.
    #
    # See https://zulip.readthedocs.io/en/latest/subsystems/sending-messages.html#soft-deactivation
    # for details on this system.
    user_messages = []
    for um in ums_to_create:
        if (um.user_profile_id in long_term_idle_user_ids and
                um.user_profile_id not in stream_push_user_ids and
                um.user_profile_id not in stream_email_user_ids and
                message.is_stream_message() and
                int(um.flags) == 0):
            continue
        user_messages.append(um)

    return user_messages

def bulk_insert_ums(ums: List[UserMessageLite]) -> None:
    '''
    Doing bulk inserts this way is much faster than using Django,
    since we don't have any ORM overhead.  Profiling with 1000
    users shows a speedup of 0.436 -> 0.027 seconds, so we're
    talking about a 15x speedup.
    '''
    if not ums:
        return

    vals = [
        (um.user_profile_id, um.message_id, um.flags)
        for um in ums
    ]
    query = SQL('''
        INSERT into
            zerver_usermessage (user_profile_id, message_id, flags)
        VALUES %s
    ''')

    with connection.cursor() as cursor:
        execute_values(cursor.cursor, query, vals)

def do_add_submessage(realm: Realm,
                      sender_id: int,
                      message_id: int,
                      msg_type: str,
                      content: str,
                      ) -> None:
    submessage = SubMessage(
        sender_id=sender_id,
        message_id=message_id,
        msg_type=msg_type,
        content=content,
    )
    submessage.save()

    event = dict(
        type="submessage",
        msg_type=msg_type,
        message_id=message_id,
        submessage_id=submessage.id,
        sender_id=sender_id,
        content=content,
    )
    ums = UserMessage.objects.filter(message_id=message_id)
    target_user_ids = [um.user_profile_id for um in ums]

    send_event(realm, event, target_user_ids)

def notify_reaction_update(user_profile: UserProfile, message: Message,
                           reaction: Reaction, op: str) -> None:
    user_dict = {'user_id': user_profile.id,
                 'email': user_profile.email,
                 'full_name': user_profile.full_name}

    event: Dict[str, Any] = {
        'type': 'reaction',
        'op': op,
        'user_id': user_profile.id,
        # TODO: We plan to remove this redundant user_dict object once
        # clients are updated to support accessing use user_id.  See
        # https://github.com/zulip/zulip/pull/14711 for details.
        'user': user_dict,
        'message_id': message.id,
        'emoji_name': reaction.emoji_name,
        'emoji_code': reaction.emoji_code,
        'reaction_type': reaction.reaction_type,
    }

    # Update the cached message since new reaction is added.
    update_to_dict_cache([message])

    # Recipients for message update events, including reactions, are
    # everyone who got the original message.  This means reactions
    # won't live-update in preview narrows, but it's the right
    # performance tradeoff, since otherwise we'd need to send all
    # reactions to public stream messages to every browser for every
    # client in the organization, which doesn't scale.
    #
    # However, to ensure that reactions do live-update for any user
    # who has actually participated in reacting to a message, we add a
    # "historical" UserMessage row for any user who reacts to message,
    # subscribing them to future notifications.
    ums = UserMessage.objects.filter(message=message.id)
    send_event(user_profile.realm, event, [um.user_profile_id for um in ums])

def do_add_reaction_legacy(user_profile: UserProfile, message: Message, emoji_name: str) -> None:
    (emoji_code, reaction_type) = emoji_name_to_emoji_code(user_profile.realm, emoji_name)
    reaction = Reaction(user_profile=user_profile, message=message,
                        emoji_name=emoji_name, emoji_code=emoji_code,
                        reaction_type=reaction_type)
    try:
        reaction.save()
    except django.db.utils.IntegrityError:  # nocoverage
        # This can happen when a race results in the check in views
        # code not catching an attempt to double-add a reaction, or
        # perhaps if the emoji_name/emoji_code mapping is busted.
        raise JsonableError(_("Reaction already exists."))

    notify_reaction_update(user_profile, message, reaction, "add")

def do_remove_reaction_legacy(user_profile: UserProfile, message: Message, emoji_name: str) -> None:
    reaction = Reaction.objects.get(user_profile=user_profile,
                                       message=message,
                                       emoji_name=emoji_name)
    reaction.delete()
    notify_reaction_update(user_profile, message, reaction, "remove")

def do_add_reaction(user_profile: UserProfile, message: Message,
                    emoji_name: str, emoji_code: str, reaction_type: str) -> None:
    reaction = Reaction(user_profile=user_profile, message=message,
                        emoji_name=emoji_name, emoji_code=emoji_code,
                        reaction_type=reaction_type)
    try:
        reaction.save()
    except django.db.utils.IntegrityError:  # nocoverage
        # This can happen when a race results in the check in views
        # code not catching an attempt to double-add a reaction, or
        # perhaps if the emoji_name/emoji_code mapping is busted.
        raise JsonableError(_("Reaction already exists."))

    notify_reaction_update(user_profile, message, reaction, "add")

def do_remove_reaction(user_profile: UserProfile, message: Message,
                       emoji_code: str, reaction_type: str) -> None:
    reaction = Reaction.objects.get(user_profile=user_profile,
                                       message=message,
                                       emoji_code=emoji_code,
                                       reaction_type=reaction_type)
    reaction.delete()
    notify_reaction_update(user_profile, message, reaction, "remove")


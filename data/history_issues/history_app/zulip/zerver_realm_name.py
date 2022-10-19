from __future__ import absolute_import

from django.db import models
from django.conf import settings
from django.contrib.auth.models import AbstractBaseUser, UserManager, \
    PermissionsMixin
from zerver.lib.cache import cache_with_key, update_user_profile_cache, \
    user_profile_by_id_cache_key, user_profile_by_email_cache_key, \
    generic_bulk_cached_fetch, cache_set, \
    display_recipient_cache_key
from zerver.lib.utils import make_safe_digest, generate_random_token
from django.db import transaction, IntegrityError
from zerver.lib import bugdown
from zerver.lib.avatar import gravatar_hash, get_avatar_url
from django.utils import timezone
from django.contrib.sessions.models import Session
from zerver.lib.timestamp import datetime_to_timestamp
from django.db.models.signals import post_save, post_delete
import zlib

from bitfield import BitField
from collections import defaultdict
import pylibmc
import ujson
import logging

MAX_SUBJECT_LENGTH = 60
MAX_MESSAGE_LENGTH = 10000

def is_super_user(user):
    return user.email in ["tabbott/extra@mit.edu", "emailgateway@zulip.com"]

# Doing 1000 memcached requests to get_display_recipient is quite slow,
# so add a local cache as well as the memcached cache.
per_process_display_recipient_cache = {}
def get_display_recipient_by_id(recipient_id, recipient_type, recipient_type_id):
    if recipient_id not in per_process_display_recipient_cache:
        result = get_display_recipient_memcached(recipient_id, recipient_type, recipient_type_id)
        per_process_display_recipient_cache[recipient_id] = result
    return per_process_display_recipient_cache[recipient_id]

def get_display_recipient(recipient):
    return get_display_recipient_by_id(
            recipient.id,
            recipient.type,
            recipient.type_id
    )

def flush_per_process_display_recipient_cache():
    global per_process_display_recipient_cache
    per_process_display_recipient_cache = {}

@cache_with_key(lambda *args: display_recipient_cache_key(args[0]),
                timeout=3600*24*7)
def get_display_recipient_memcached(recipient_id, recipient_type, recipient_type_id):
    """
    returns: an appropriate object describing the recipient.  For a
    stream this will be the stream name as a string.  For a huddle or
    personal, it will be an array of dicts about each recipient.
    """
    if recipient_type == Recipient.STREAM:
        stream = Stream.objects.get(id=recipient_type_id)
        return stream.name

    # We don't really care what the ordering is, just that it's deterministic.
    user_profile_list = (UserProfile.objects.filter(subscription__recipient_id=recipient_id)
                                            .select_related()
                                            .order_by('email'))
    return [{'email': user_profile.email,
             'domain': user_profile.realm.domain,
             'full_name': user_profile.full_name,
             'short_name': user_profile.short_name,
             'id': user_profile.id} for user_profile in user_profile_list]

def completely_open(domain):
    # This domain is completely open to everyone on the internet to
    # join. This is not the same as a "restricted_to_domain" realm: in
    # those realms, users from outside the domain must be invited.
    return domain and domain.lower() == "customer3.invalid"


def get_realm_emoji_cache_key(realm):
    return 'realm_emoji:%s' % (realm.id,)

class Realm(models.Model):
    # domain is a domain in the Internet sense. It must be structured like a
    # valid email domain. We use is to restrict access, identify bots, etc.
    domain = models.CharField(max_length=40, db_index=True, unique=True)
    # name is the user-visible identifier for the realm. It has no required
    # structure.
    name = models.CharField(max_length=40, null=True)
    restricted_to_domain = models.BooleanField(default=True)
    date_created = models.DateTimeField(default=timezone.now)
    notifications_stream = models.ForeignKey('Stream', related_name='+', null=True, blank=True)

    NOTIFICATION_STREAM_NAME = 'zulip'

    def __repr__(self):
        return (u"<Realm: %s %s>" % (self.domain, self.id)).encode("utf-8")
    def __str__(self):
        return self.__repr__()

    @cache_with_key(get_realm_emoji_cache_key, timeout=3600*24*7)
    def get_emoji(self):
        return get_realm_emoji_uncached(self)

    class Meta:
        permissions = (
            ('administer', "Administer a realm"),
        )

# These functions should only be used on email addresses that have
# been validated via django.core.validators.validate_email
#
# Note that we need to use some care, since can you have multiple @-signs; e.g.
# "tabbott@test"@zulip.com
# is valid email address
def email_to_username(email):
    return "@".join(email.split("@")[:-1]).lower()

def email_to_domain(email):
    return email.split("@")[-1].lower()

class RealmEmoji(models.Model):
    realm = models.ForeignKey(Realm)
    name = models.TextField()
    img_url = models.TextField()

    class Meta:
        unique_together = ("realm", "name")

    def __str__(self):
        return "<RealmEmoji(%s): %s %s>" % (self.realm.domain, self.name, self.img_url)

def get_realm_emoji_uncached(realm):
    d = {}
    for row in RealmEmoji.objects.filter(realm=realm):
        d[row.name] = row.img_url
    return d

def update_realm_emoji_cache(sender, **kwargs):
    realm = kwargs['instance'].realm
    cache_set(get_realm_emoji_cache_key(realm),
              get_realm_emoji_uncached(realm),
              timeout=3600*24*7)

post_save.connect(update_realm_emoji_cache, sender=RealmEmoji)
post_delete.connect(update_realm_emoji_cache, sender=RealmEmoji)

class UserProfile(AbstractBaseUser, PermissionsMixin):
    # Fields from models.AbstractUser minus last_name and first_name,
    # which we don't use; email is modified to make it indexed and unique.
    email = models.EmailField(blank=False, db_index=True, unique=True)
    is_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    is_bot = models.BooleanField(default=False)
    date_joined = models.DateTimeField(default=timezone.now)
    bot_owner = models.ForeignKey('self', null=True, on_delete=models.SET_NULL)

    USERNAME_FIELD = 'email'
    MAX_NAME_LENGTH = 100

    # Our custom site-specific fields
    full_name = models.CharField(max_length=MAX_NAME_LENGTH)
    short_name = models.CharField(max_length=MAX_NAME_LENGTH)
    # pointer points to Message.id, NOT UserMessage.id.
    pointer = models.IntegerField()
    last_pointer_updater = models.CharField(max_length=64)
    realm = models.ForeignKey(Realm)
    api_key = models.CharField(max_length=32)
    enable_desktop_notifications = models.BooleanField(default=True)
    enable_sounds = models.BooleanField(default=True)
    enter_sends = models.NullBooleanField(default=False)
    enable_offline_email_notifications = models.BooleanField(default=True)
    last_reminder = models.DateTimeField(default=timezone.now, null=True)
    rate_limits = models.CharField(default="", max_length=100) # comma-separated list of range:max pairs

    # Hours to wait before sending another email to a user
    EMAIL_REMINDER_WAITPERIOD = 24
    # Minutes to wait before warning a bot owner that her bot sent a message
    # to a nonexistent stream
    BOT_OWNER_STREAM_ALERT_WAITPERIOD = 1

    AVATAR_FROM_GRAVATAR = 'G'
    AVATAR_FROM_USER = 'U'
    AVATAR_FROM_SYSTEM = 'S'
    AVATAR_SOURCES = (
            (AVATAR_FROM_GRAVATAR, 'Hosted by Gravatar'),
            (AVATAR_FROM_USER, 'Uploaded by user'),
            (AVATAR_FROM_SYSTEM, 'System generated'),
    )
    avatar_source = models.CharField(default=AVATAR_FROM_GRAVATAR, choices=AVATAR_SOURCES, max_length=1)

    TUTORIAL_WAITING  = 'W'
    TUTORIAL_STARTED  = 'S'
    TUTORIAL_FINISHED = 'F'
    TUTORIAL_STATES   = ((TUTORIAL_WAITING,  "Waiting"),
                         (TUTORIAL_STARTED,  "Started"),
                         (TUTORIAL_FINISHED, "Finished"))

    tutorial_status = models.CharField(default=TUTORIAL_WAITING, choices=TUTORIAL_STATES, max_length=1)
    # Contains serialized JSON of the form:
    #    [("step 1", true), ("step 2", false)]
    # where the second element of each tuple is if the step has been
    # completed.
    onboarding_steps = models.TextField(default=ujson.dumps([]))

    invites_granted = models.IntegerField(default=0)
    invites_used = models.IntegerField(default=0)

    alert_words = models.TextField(default=ujson.dumps([])) # json-serialized list of strings

    # Contains serialized JSON of the form:
    # [["social", "mit"], ["devel", "ios"]]
    muted_topics = models.TextField(default=ujson.dumps([]))

    objects = UserManager()

    def can_admin_user(self, target_user):
        """Returns whether this user has permission to modify target_user"""
        if target_user.bot_owner == self:
            return True
        elif self.has_perm('administer', target_user.realm):
            return True
        else:
            return False

    @property
    def show_admin(self):
        # Logic to determine if the user should see the administration tools.
        # Do NOT use this to check if a user is authorized to perform a specific action!
        return 0 < self.userobjectpermission_set.filter(
                content_type__name="realm",
                permission__codename="administer").count()

    @property
    def public_streams_disabled(self):
        return self.email.lower() == "restricted-user@customer5.invalid"

    def last_reminder_tzaware(self):
        if self.last_reminder is not None and timezone.is_naive(self.last_reminder):
            logging.warning("Loaded a user_profile.last_reminder for user %s that's not tz-aware: %s"
                              % (self.email, self.last_reminder))
            return self.last_reminder.replace(tzinfo=timezone.utc)

        return self.last_reminder

    def __repr__(self):
        return (u"<UserProfile: %s %s>" % (self.email, self.realm)).encode("utf-8")
    def __str__(self):
        return self.__repr__()

# Make sure we flush the UserProfile object from our memcached
# whenever we save it.
post_save.connect(update_user_profile_cache, sender=UserProfile)

class PreregistrationUser(models.Model):
    email = models.EmailField()
    referred_by = models.ForeignKey(UserProfile, null=True)
    streams = models.ManyToManyField('Stream', null=True)
    invited_at = models.DateTimeField(auto_now=True)

    # status: whether an object has been confirmed.
    #   if confirmed, set to confirmation.settings.STATUS_ACTIVE
    status = models.IntegerField(default=0)

    realm = models.ForeignKey(Realm, null=True)

class AppleDeviceToken(models.Model):
    # The token is a unique device-specific token that is
    # sent to us from each iOS device, after registering with
    # the APNS service
    token = models.CharField(max_length=255, unique=True)

    # The user who's device this is
    user = models.ForeignKey(UserProfile, db_index=True)

class MitUser(models.Model):
    email = models.EmailField(unique=True)
    # status: whether an object has been confirmed.
    #   if confirmed, set to confirmation.settings.STATUS_ACTIVE
    status = models.IntegerField(default=0)

class Stream(models.Model):
    MAX_NAME_LENGTH = 60
    name = models.CharField(max_length=MAX_NAME_LENGTH, db_index=True)
    realm = models.ForeignKey(Realm, db_index=True)
    invite_only = models.NullBooleanField(default=False)
    # Used by the e-mail forwarder. The e-mail RFC specifies a maximum
    # e-mail length of 254, and our max stream length is 30, so we
    # have plenty of room for the token.
    email_token = models.CharField(
        max_length=32, default=lambda: generate_random_token(32))

    date_created = models.DateTimeField(default=timezone.now)

    def __repr__(self):
        return (u"<Stream: %s>" % (self.name,)).encode("utf-8")
    def __str__(self):
        return self.__repr__()

    def is_public(self):
        # For every realm except for legacy realms on prod (aka those
        # older than realm id 68 with some exceptions), we enable
        # historical messages for all streams that are not invite-only.
        return ((not settings.DEPLOYED or self.realm.domain in
                 ["zulip.com"] or self.realm.id > 68
                 or settings.LOCALSERVER)
                and not self.invite_only)

    class Meta:
        unique_together = ("name", "realm")

    @classmethod
    def create(cls, name, realm):
        stream = cls(name=name, realm=realm)
        stream.save()

        recipient = Recipient.objects.create(type_id=stream.id,
                                             type=Recipient.STREAM)
        return (stream, recipient)

    def num_subscribers(self):
        return Subscription.objects.filter(
                recipient__type=Recipient.STREAM,
                recipient__type_id=self.id,
                user_profile__is_active=True,
                active=True
        ).count()

def valid_stream_name(name):
    return name != ""

class Recipient(models.Model):
    type_id = models.IntegerField(db_index=True)
    type = models.PositiveSmallIntegerField(db_index=True)
    # Valid types are {personal, stream, huddle}
    PERSONAL = 1
    STREAM = 2
    HUDDLE = 3

    class Meta:
        unique_together = ("type", "type_id")

    # N.B. If we used Django's choice=... we would get this for free (kinda)
    _type_names = {
        PERSONAL: 'personal',
        STREAM:   'stream',
        HUDDLE:   'huddle' }

    def type_name(self):
        # Raises KeyError if invalid
        return self._type_names[self.type]

    def __repr__(self):
        display_recipient = get_display_recipient(self)
        return (u"<Recipient: %s (%d, %s)>" % (display_recipient, self.type_id, self.type)).encode("utf-8")

class Client(models.Model):
    name = models.CharField(max_length=30, db_index=True, unique=True)

def get_client_cache_key(name):
    return 'get_client:%s' % (make_safe_digest(name),)

@cache_with_key(get_client_cache_key, timeout=3600*24*7)
@transaction.commit_on_success
def get_client(name):
    try:
        (client, _) = Client.objects.get_or_create(name=name)
    except IntegrityError:
        # If we're racing with other threads trying to create this
        # client, get_or_create will throw IntegrityError (because our
        # database is enforcing the no-duplicate-objects constraint);
        # in this case one should just re-fetch the object.  This race
        # actually happens with populate_db.
        #
        # Much of the rest of our code that writes to the database
        # doesn't handle this duplicate object on race issue correctly :(
        transaction.commit()
        return Client.objects.get(name=name)
    return client

def get_stream_cache_key(stream_name, realm):
    if isinstance(realm, Realm):
        realm_id = realm.id
    else:
        realm_id = realm
    return "stream_by_realm_and_name:%s:%s" % (
        realm_id, make_safe_digest(stream_name.strip().lower()))

# get_stream_backend takes either a realm id or a realm
@cache_with_key(get_stream_cache_key, timeout=3600*24*7)
def get_stream_backend(stream_name, realm):
    if isinstance(realm, Realm):
        realm_id = realm.id
    else:
        realm_id = realm
    return Stream.objects.select_related("realm").get(
        name__iexact=stream_name.strip(), realm_id=realm_id)

# get_stream takes either a realm id or a realm
def get_stream(stream_name, realm):
    try:
        return get_stream_backend(stream_name, realm)
    except Stream.DoesNotExist:
        return None

def bulk_get_streams(realm, stream_names):
    if isinstance(realm, Realm):
        realm_id = realm.id
    else:
        realm_id = realm

    def fetch_streams_by_name(stream_names):
        # This should be just
        #
        # Stream.objects.select_related("realm").filter(name__iexact__in=stream_names,
        #                                               realm_id=realm_id)
        #
        # But chaining __in and __iexact doesn't work with Django's
        # ORM, so we have the following hack to construct the relevant where clause
        if len(stream_names) == 0:
            return []
        upper_list = ", ".join(["UPPER(%s)"] * len(stream_names))
        where_clause = "UPPER(zerver_stream.name::text) IN (%s)" % (upper_list,)
        return Stream.objects.select_related("realm").filter(realm_id=realm_id).extra(
            where=[where_clause],
            params=stream_names)

    return generic_bulk_cached_fetch(lambda stream_name: get_stream_cache_key(stream_name, realm),
                                     fetch_streams_by_name,
                                     [stream_name.lower() for stream_name in stream_names],
                                     id_fetcher=lambda stream: stream.name.lower())

def get_recipient_cache_key(type, type_id):
    return "get_recipient:%s:%s" % (type, type_id,)

@cache_with_key(get_recipient_cache_key, timeout=3600*24*7)
def get_recipient(type, type_id):
    return Recipient.objects.get(type_id=type_id, type=type)

def bulk_get_recipients(type, type_ids):
    def cache_key_function(type_id):
        return get_recipient_cache_key(type, type_id)
    def query_function(type_ids):
        return Recipient.objects.filter(type=type, type_id__in=type_ids)

    return generic_bulk_cached_fetch(cache_key_function, query_function, type_ids,
                                     id_fetcher=lambda recipient: recipient.type_id)

# NB: This function is currently unused, but may come in handy.
def linebreak(string):
    return string.replace('\n\n', '<p/>').replace('\n', '<br/>')

def extract_message_dict(message_str):
    return ujson.loads(zlib.decompress(message_str))

def stringify_message_dict(message_dict):
    return zlib.compress(ujson.dumps(message_dict))

def to_dict_cache_key_id(message_id, apply_markdown):
    return 'message_dict:%d:%d' % (message_id, apply_markdown)

def to_dict_cache_key(message, apply_markdown):
    return to_dict_cache_key_id(message.id, apply_markdown)

class Message(models.Model):
    sender = models.ForeignKey(UserProfile)
    recipient = models.ForeignKey(Recipient)
    subject = models.CharField(max_length=MAX_SUBJECT_LENGTH, db_index=True)
    content = models.TextField()
    rendered_content = models.TextField(null=True)
    rendered_content_version = models.IntegerField(null=True)
    pub_date = models.DateTimeField('date published', db_index=True)
    sending_client = models.ForeignKey(Client)
    last_edit_time = models.DateTimeField(null=True)
    edit_history = models.TextField(null=True)

    def __repr__(self):
        display_recipient = get_display_recipient(self.recipient)
        return (u"<Message: %s / %s / %r>" % (display_recipient, self.subject, self.sender)).encode("utf-8")
    def __str__(self):
        return self.__repr__()

    def get_realm(self):
        return self.sender.realm

    def render_markdown(self, content, domain=None):
        """Return HTML for given markdown. Bugdown may add properties to the
        message object such as `mentions_user_ids` and `mentions_wildcard`.
        These are only on this Django object and are not saved in the
        database.
        """

        self.mentions_wildcard = False
        self.mentions_user_ids = set()
        self.user_ids_with_alert_words = set()

        if not domain:
            domain = self.sender.realm.domain
        if self.sending_client.name == "zephyr_mirror" and domain == "mit.edu":
            # Use slightly customized Markdown processor for content
            # delivered via zephyr_mirror
            domain = "mit.edu/zephyr_mirror"
        return bugdown.convert(content, domain, self)

    def set_rendered_content(self, rendered_content, save = False):
        """Set the content on the message.
        """

        self.rendered_content = rendered_content
        self.rendered_content_version = bugdown.version

        if self.rendered_content is not None:
            if save:
                self.save_rendered_content()
            return True
        else:
            return False

    def save_rendered_content(self):
        self.save(update_fields=["rendered_content", "rendered_content_version"])

    def maybe_render_content(self, domain, save = False):
        """Render the markdown if there is no existing rendered_content"""
        if Message.need_to_render_content(self.rendered_content, self.rendered_content_version):
            return self.set_rendered_content(self.render_markdown(self.content, domain), save)
        else:
            return True

    @staticmethod
    def need_to_render_content(rendered_content, rendered_content_version):
        return rendered_content_version < bugdown.version or rendered_content is None

    def to_dict(self, apply_markdown):
        return extract_message_dict(self.to_dict_json(apply_markdown))

    @cache_with_key(to_dict_cache_key, timeout=3600*24)
    def to_dict_json(self, apply_markdown):
        return stringify_message_dict(self.to_dict_uncached(apply_markdown))

    def to_dict_uncached(self, apply_markdown):
        return Message.build_message_dict(
                apply_markdown = apply_markdown,
                message = self,
                message_id = self.id,
                last_edit_time = self.last_edit_time,
                edit_history = self.edit_history,
                content = self.content,
                subject = self.subject,
                pub_date = self.pub_date,
                rendered_content = self.rendered_content,
                rendered_content_version = self.rendered_content_version,
                sender_id = self.sender.id,
                sender_email = self.sender.email,
                sender_realm_domain = self.sender.realm.domain,
                sender_full_name = self.sender.full_name,
                sender_short_name = self.sender.short_name,
                sender_avatar_source = self.sender.avatar_source,
                sending_client_name = self.sending_client.name,
                recipient_id = self.recipient.id,
                recipient_type = self.recipient.type,
                recipient_type_id = self.recipient.type_id,
        )

    @staticmethod
    def build_dict_from_raw_db_row(row, apply_markdown):
        '''
        row is a row from a .values() call, and it needs to have
        all the relevant fields populated
        '''
        return Message.build_message_dict(
                apply_markdown = apply_markdown,
                message = None,
                message_id = row['id'],
                last_edit_time = row['last_edit_time'],
                edit_history = row['edit_history'],
                content = row['content'],
                subject = row['subject'],
                pub_date = row['pub_date'],
                rendered_content = row['rendered_content'],
                rendered_content_version = row['rendered_content_version'],
                sender_id = row['sender_id'],
                sender_email = row['sender__email'],
                sender_realm_domain = row['sender__realm__domain'],
                sender_full_name = row['sender__full_name'],
                sender_short_name = row['sender__short_name'],
                sender_avatar_source = row['sender__avatar_source'],
                sending_client_name = row['sending_client__name'],
                recipient_id = row['recipient_id'],
                recipient_type = row['recipient__type'],
                recipient_type_id = row['recipient__type_id'],
        )

    @staticmethod
    def build_message_dict(
            apply_markdown,
            message,
            message_id,
            last_edit_time,
            edit_history,
            content,
            subject,
            pub_date,
            rendered_content,
            rendered_content_version,
            sender_id,
            sender_email,
            sender_realm_domain,
            sender_full_name,
            sender_short_name,
            sender_avatar_source,
            sending_client_name,
            recipient_id,
            recipient_type,
            recipient_type_id,
    ):

        avatar_url = get_avatar_url(sender_avatar_source, sender_email)

        display_recipient = get_display_recipient_by_id(
                recipient_id,
                recipient_type,
                recipient_type_id
        )

        if recipient_type == Recipient.STREAM:
            display_type = "stream"
        elif recipient_type in (Recipient.HUDDLE, Recipient.PERSONAL):
            display_type = "private"
            if len(display_recipient) == 1:
                # add the sender in if this isn't a message between
                # someone and his self, preserving ordering
                recip = {'email': sender_email,
                         'domain': sender_realm_domain,
                         'full_name': sender_full_name,
                         'short_name': sender_short_name,
                         'id': sender_id};
                if recip['email'] < display_recipient[0]['email']:
                    display_recipient = [recip, display_recipient[0]]
                elif recip['email'] > display_recipient[0]['email']:
                    display_recipient = [display_recipient[0], recip]

        obj = dict(
            id                = message_id,
            sender_email      = sender_email,
            sender_full_name  = sender_full_name,
            sender_short_name = sender_short_name,
            sender_domain     = sender_realm_domain,
            sender_id         = sender_id,
            type              = display_type,
            display_recipient = display_recipient,
            recipient_id      = recipient_id,
            subject           = subject,
            timestamp         = datetime_to_timestamp(pub_date),
            gravatar_hash     = gravatar_hash(sender_email), # Deprecated June 2013
            avatar_url        = avatar_url,
            client            = sending_client_name)

        obj['subject_links'] = bugdown.subject_links(sender_realm_domain.lower(), subject)

        if last_edit_time != None:
            obj['last_edit_timestamp'] = datetime_to_timestamp(last_edit_time)
            obj['edit_history'] = ujson.loads(edit_history)

        if apply_markdown:
            if Message.need_to_render_content(rendered_content, rendered_content_version):
                if message is None:
                    # We really shouldn't be rendering objects in this method, but there is
                    # a scenario where we upgrade the version of bugdown and fail to run
                    # management commands to re-render historical messages, and then we
                    # need to have side effects.  This method is optimized to not need full
                    # blown ORM objects, but the bugdown renderer is unfortunately highly
                    # coupled to Message, and we also need to persist the new rendered content.
                    # If we don't have a message object passed in, we get one here.  The cost
                    # of going to the DB here should be overshadowed by the cost of rendering
                    # and updating the row.
                    message = Message.objects.select_related().get(id=message_id)

                # It's unfortunate that we need to have side effects on the message
                # in some cases.
                rendered_content = message.render_markdown(content, sender_realm_domain)
                message.set_rendered_content(rendered_content, True)

            if rendered_content is not None:
                obj['content'] = rendered_content
            else:
                obj['content'] = '<p>[Zulip note: Sorry, we could not understand the formatting of your message]</p>'

            obj['content_type'] = 'text/html'
        else:
            obj['content'] = content
            obj['content_type'] = 'text/x-markdown'

        return obj

    def to_log_dict(self):
        return dict(
            id                = self.id,
            sender_email      = self.sender.email,
            sender_domain     = self.sender.realm.domain,
            sender_full_name  = self.sender.full_name,
            sender_short_name = self.sender.short_name,
            sending_client    = self.sending_client.name,
            type              = self.recipient.type_name(),
            recipient         = get_display_recipient(self.recipient),
            subject           = self.subject,
            content           = self.content,
            timestamp         = datetime_to_timestamp(self.pub_date))

    @staticmethod
    def get_raw_db_rows(needed_ids):
        # This is a special purpose function optimized for
        # callers like get_old_messages_backend().
        fields = [
            'id',
            'subject',
            'pub_date',
            'last_edit_time',
            'edit_history',
            'content',
            'rendered_content',
            'rendered_content_version',
            'recipient_id',
            'recipient__type',
            'recipient__type_id',
            'sender_id',
            'sending_client__name',
            'sender__email',
            'sender__full_name',
            'sender__short_name',
            'sender__realm__id',
            'sender__realm__domain',
            'sender__avatar_source'
        ]
        return Message.objects.filter(id__in=needed_ids).values(*fields)

    @classmethod
    def remove_unreachable(cls):
        """Remove all Messages that are not referred to by any UserMessage."""
        cls.objects.exclude(id__in = UserMessage.objects.values('message_id')).delete()

class UserMessage(models.Model):
    user_profile = models.ForeignKey(UserProfile)
    message = models.ForeignKey(Message)
    # We're not using the archived field for now, but create it anyway
    # since this table will be an unpleasant one to do schema changes
    # on later
    archived = models.BooleanField()
    ALL_FLAGS = ['read', 'starred', 'collapsed', 'mentioned', 'wildcard_mentioned',
                 'summarize_in_home', 'summarize_in_stream', 'force_expand', 'force_collapse',
                 'has_alert_word']
    flags = BitField(flags=ALL_FLAGS, default=0)

    class Meta:
        unique_together = ("user_profile", "message")

    def __repr__(self):
        display_recipient = get_display_recipient(self.message.recipient)
        return (u"<UserMessage: %s / %s (%s)>" % (display_recipient, self.user_profile.email, self.flags_list())).encode("utf-8")

    def flags_list(self):
        return [flag for flag in self.flags.keys() if getattr(self.flags, flag).is_set]

def parse_usermessage_flags(val):
    flags = []
    mask = 1
    for flag in UserMessage.ALL_FLAGS:
        if val & mask:
            flags.append(flag)
        mask <<= 1
    return flags

class Subscription(models.Model):
    user_profile = models.ForeignKey(UserProfile)
    recipient = models.ForeignKey(Recipient)
    active = models.BooleanField(default=True)
    in_home_view = models.NullBooleanField(default=True)

    DEFAULT_STREAM_COLOR = "#c2c2c2"
    color = models.CharField(max_length=10, default=DEFAULT_STREAM_COLOR)
    notifications = models.BooleanField(default=False)

    class Meta:
        unique_together = ("user_profile", "recipient")

    def __repr__(self):
        return (u"<Subscription: %r -> %s>" % (self.user_profile, self.recipient)).encode("utf-8")
    def __str__(self):
        return self.__repr__()

@cache_with_key(user_profile_by_id_cache_key, timeout=3600*24*7)
def get_user_profile_by_id(uid):
    return UserProfile.objects.select_related().get(id=uid)

@cache_with_key(user_profile_by_email_cache_key, timeout=3600*24*7)
def get_user_profile_by_email(email):
    return UserProfile.objects.select_related().get(email__iexact=email)

def get_active_user_profiles_by_realm(realm):
    return UserProfile.objects.select_related().filter(realm=realm,
                                                       is_active=True)

def get_prereg_user_by_email(email):
    # A user can be invited many times, so only return the result of the latest
    # invite.
    return PreregistrationUser.objects.filter(email__iexact=email).latest("invited_at")

class Huddle(models.Model):
    # TODO: We should consider whether using
    # CommaSeparatedIntegerField would be better.
    huddle_hash = models.CharField(max_length=40, db_index=True, unique=True)

def get_huddle_hash(id_list):
    id_list = sorted(set(id_list))
    hash_key = ",".join(str(x) for x in id_list)
    return make_safe_digest(hash_key)

def huddle_hash_cache_key(huddle_hash):
    return "huddle_by_hash:%s" % (huddle_hash,)

def get_huddle(id_list):
    huddle_hash = get_huddle_hash(id_list)
    return get_huddle_backend(huddle_hash, id_list)

@cache_with_key(lambda huddle_hash, id_list: huddle_hash_cache_key(huddle_hash), timeout=3600*24*7)
def get_huddle_backend(huddle_hash, id_list):
    (huddle, created) = Huddle.objects.get_or_create(huddle_hash=huddle_hash)
    if created:
        with transaction.commit_on_success():
            recipient = Recipient.objects.create(type_id=huddle.id,
                                                 type=Recipient.HUDDLE)
            subs_to_create = [Subscription(recipient=recipient,
                                           user_profile=get_user_profile_by_id(user_profile_id))
                              for user_profile_id in id_list]
            Subscription.objects.bulk_create(subs_to_create)
    return huddle

def get_realm(domain):
    if not domain:
        return None
    try:
        return Realm.objects.get(domain__iexact=domain.strip())
    except Realm.DoesNotExist:
        return None

def clear_database():
    pylibmc.Client(['127.0.0.1']).flush_all()
    for model in [Message, Stream, UserProfile, Recipient,
                  Realm, Subscription, Huddle, UserMessage, Client,
                  DefaultStream]:
        model.objects.all().delete()
    Session.objects.all().delete()

class UserActivity(models.Model):
    user_profile = models.ForeignKey(UserProfile)
    client = models.ForeignKey(Client)
    query = models.CharField(max_length=50, db_index=True)

    count = models.IntegerField()
    last_visit = models.DateTimeField('last visit')

    class Meta:
        unique_together = ("user_profile", "client", "query")

class UserActivityInterval(models.Model):
    user_profile = models.ForeignKey(UserProfile)
    start = models.DateTimeField('start time', db_index=True)
    end = models.DateTimeField('end time', db_index=True)

class UserPresence(models.Model):
    user_profile = models.ForeignKey(UserProfile)
    client = models.ForeignKey(Client)

    # Valid statuses
    ACTIVE = 1
    IDLE = 2

    timestamp = models.DateTimeField('presence changed')
    status = models.PositiveSmallIntegerField(default=ACTIVE)

    @staticmethod
    def status_to_string(status):
        if status == UserPresence.ACTIVE:
            return 'active'
        elif status == UserPresence.IDLE:
            return 'idle'

    @staticmethod
    def get_status_dict_by_realm(realm_id):
        user_statuses = defaultdict(dict)

        query = UserPresence.objects.filter(
                user_profile__realm_id=realm_id,
                user_profile__is_active=True,
                user_profile__is_bot=False
        ).values(
                'client__name',
                'status',
                'timestamp',
                'user_profile__email'
        )

        for row in query:
            email = row['user_profile__email']
            client_name = row['client__name']
            status = row['status']
            timestamp = row['timestamp']
            info = UserPresence.to_presense_dict(client_name, status, timestamp)
            user_statuses[email][client_name] = info

        return user_statuses

    @staticmethod
    def to_presense_dict(client_name, status, timestamp):
        presence_val = UserPresence.status_to_string(status)
        timestamp = datetime_to_timestamp(timestamp)
        return dict(
                client=client_name,
                status=presence_val,
                timestamp=timestamp
        )

    def to_dict(self):
        return UserPresence.to_presense_dict(
                self.client.name,
                self.status,
                self.timestamp
        )

    @staticmethod
    def status_from_string(status):
        if status == 'active':
            status_val = UserPresence.ACTIVE
        elif status == 'idle':
            status_val = UserPresence.IDLE
        else:
            status_val = None

        return status_val

    class Meta:
        unique_together = ("user_profile", "client")

class DefaultStream(models.Model):
    realm = models.ForeignKey(Realm)
    stream = models.ForeignKey(Stream)

    class Meta:
        unique_together = ("realm", "stream")

# FIXME: The foreign key relationship here is backwards.
#
# We can't easily get a list of streams and their associated colors (if any) in
# a single query.  See zerver.views.gather_subscriptions for an example.
#
# We should change things around so that is possible.  Probably this should
# just be a column on Subscription.
class StreamColor(models.Model):
    DEFAULT_STREAM_COLOR = "#c2c2c2"

    subscription = models.ForeignKey(Subscription)
    color = models.CharField(max_length=10)

class Referral(models.Model):
    user_profile = models.ForeignKey(UserProfile)
    email = models.EmailField(blank=False, null=False)
    timestamp = models.DateTimeField(auto_now_add=True, null=False)
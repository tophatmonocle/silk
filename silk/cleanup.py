""" Cleanup and maintenance operations. """

from datetime import datetime

from silk import models

DEFAULT_CHUNKSIZE = 1000


def _delete_by_key(model, key, values):
    """Delete all model objects where key is one of values."""
    criteria = {}
    criteria['{}__in'.format(key)] = values
    model.objects.filter(**criteria).delete()


def _delete_requests(ids):
    '''
    Clean up a chunk of requests by their ids.

    The implementation of django's CASCADE emulation makes bulk
    deletion quite slow. It issues UPDATE SET NULL for nullable foreign keys
    prior to deletion, for example. Let's break this up a bit.

    This still does trigger some stray SELECT right
    before running the Request DELETE.
    '''
    dependent_models = [models.Profile, models.SQLQuery, models.Response]

    for model in dependent_models:
        _delete_by_key(model, 'request_id', ids)

    _delete_by_key(models.Request, 'pk', ids)


def _cleanup(horizon=None, request_chunk_size=DEFAULT_CHUNKSIZE):
    queryset = models.Request.objects.values_list('pk', flat=True)

    if horizon is not None:
        cutoff = datetime.now() - horizon
        queryset = queryset.filter(start_time__lt=cutoff)

    deleted_requests = 0

    while True:
        expired_request_ids = list(
            queryset.order_by('start_time')[:request_chunk_size])

        if not expired_request_ids:
            break

        _delete_requests(expired_request_ids)
        deleted_requests += len(expired_request_ids)

    return deleted_requests


def cleanup_expired_requests(horizon, request_chunk_size=DEFAULT_CHUNKSIZE):
    """
    Delete request data older than the timedelta specified in horizon.

    This is a public interface and can be called by the django project, for
    example in a celerybeat job.
    """
    return _cleanup(horizon=horizon, request_chunk_size=request_chunk_size)


def purge():
    """Delete all request data."""
    return _cleanup()

from datetime import timedelta
from optparse import make_option

from django.core.management.base import BaseCommand

from silk.cleanup import purge, cleanup_expired_requests


class Command(BaseCommand):
    help = "Clears silk's log of requests."
    option_list = BaseCommand.option_list + (
        make_option(
            '--older-than',
            type="int",
            help='Delete requests older than X time units',
        ),
        make_option(
            '--time-unit',
            default='days',
            choices=['seconds', 'minutes', 'hours', 'days', 'weeks'],
            help='Time unit for the deletion horizon count',
        )
    )

    def handle(self, *args, **options):
        if options['older_than']:
            delta_args = {}
            delta_args[options['time_unit']] = options['older_than']
            deleted_requests = cleanup_expired_requests(
                timedelta(**delta_args))
        else:
            deleted_requests = purge()

        self.stdout.write(
            'Successfully deleted {} requests'.format(deleted_requests))

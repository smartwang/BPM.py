from django.db import models


class Defination(models.Model):

    name = models.SlugField(
        max_length=255,
    )
    category = models.PositiveSmallIntegerField(
        choices=(
            (0, 'PROCESS'),
            (1, 'COMPONENT'),
        )
    )
    content = models.TextField()


class Process(models.Model):

    defination = models.ForeignKey(Defination)
    state = models.PositiveSmallIntegerField(
        choices=(
            (0, 'STARTED'),
            (1, 'PAUSED'),
            (2, 'COMPLETE'),
            (3, 'TERMINATED'),
            (4, 'INTERRUPTED'),
            (5, 'REVOKED'),
        ),
        default=0,
    )
    # is_subprocess = models.BooleanField(
    #     default=False,
    # )

    parent_process = models.ForeignKey('self', null=True)
    is_locked = models.BooleanField(
        default=False,
    )
    pickled = models.TextField(db_column='pickle')


class Task(models.Model):

    process = models.ForeignKey(Process)
    args = models.TextField()
    kwargs = models.TextField()
    is_complete = models.BooleanField(
        default=False,
    )
    is_confirmed = models.BooleanField(
        default=False,
    )
    result = models.TextField()

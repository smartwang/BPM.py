# coding: utf-8
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

    def __unicode__(self):
        return self.name


class Task(models.Model):
    """
    任务实例表
    记录了过程或者组件的调用实例的相关参数、状态及结果
    """
    args = models.TextField()
    kwargs = models.TextField()
    is_complete = models.BooleanField(
        default=False,
    )
    is_confirmed = models.BooleanField(
        default=False,
    )
    process = models.ForeignKey('Process', verbose_name=u'所属process', related_name=u'process')
    result = models.TextField()

    def __unicode__(self):
        return  u"%s的task" %self.process.defination.name


class Process(models.Model):
    """过程实例表"""
    defination = models.ForeignKey(Defination)
    task = models.OneToOneField(Task, related_name=u'task', null=True)  # 过程也属于一种task, 且一个过程实例就只能有一个task实例
    state = models.PositiveSmallIntegerField(
        choices=(
            (0, 'STARTED'),
            (1, 'PAUSED'),
            (2, 'COMPLETED'),
            (3, 'TERMINATED'),
            (4, 'INTERRUPTED'),
            (5, 'REVOKED'),
        ),
        default=0,
    )
    # parent_process = models.ForeignKey('self', null=True) # 似乎没有必要记录子过程的父过程，因为子过程一旦state=2后，依据task_id进行回调就能解决通知的问题了
    is_locked = models.BooleanField(
        default=False,
    )
    pickled = models.TextField(db_column='pickle')
    def __unicode__(self):
        return  u"%s的process" %self.defination.name

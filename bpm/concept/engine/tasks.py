# coding: utf-8
import cPickle
import pickle
import stackless

from celery import task

from .models import Defination, Process, Task
from .process import join, BaseProcess


@task()
def task_manager(name, task_id):    # name 应该是完整名称
    try:
        defination = Defination.objects.get(name=name)
    except Defination.DoesNotExist:
        pass
    else:
        ns = {
            '__builtins__': None,
            '__name__': __name__,
            'BaseProcess': BaseProcess,     # TODO: 添加BaseComponent
            'join': 'join',
            }
        code = compile(defination.content, '<stdin>', 'exec')

        try:
            exec code in ns
        except:
            pass

        cls = ns[name]
        globals()[name] = cls   # 这个class可能是一个过程(继承于BaseProcess)，也可能是一个组件(继承于BaseComponent)

        try:
            t = Task.objects.get(pk=task_id)
        except:
            pass    # TODO

        try:
            args = pickle.loads(t.args)
            kwargs = pickle.loads(t.kwargs)
        except:
            args = None
            kwargs = None
            pass    # TODO error log

        # cls.start(args, kwargs)
        # callback.apply_async(args=(task_id, ))
        if issubclass(cls, BaseProcess):    # TODO: 检查是否为子过程以及调用权限
            start.delay(name, task_id=task_id, *args, **kwargs)


@task()
def schedule(process_id):

    try:
        p = Process.objects.get(pk=process_id)
    except Process.DoesNotExist:
        return
    else:
        rows = Process.objects.filter(pk=p.pk, is_locked=False).update(is_locked=True)

    if rows:
        print "#" * 40
        print "Got lock"
        print "#" * 40
        ns = {
            '__builtins__': None,
            '__name__': __name__,
            'BaseProcess': BaseProcess,
            'join': 'join',
        }
        code = compile(p.defination.content, '<stdin>', 'exec')

        try:
            exec code in ns
        except:
            pass

        globals()[p.defination.name] = ns[p.defination.name]

        process = cPickle.loads(str(p.pickled))
        process.resume()

        stackless.schedule()
        while process.can_continue():
            print "#" * 40
            print "Process %d can continue" % process.process_id
            print "#" * 40
            stackless.schedule()

        Process.objects.filter(pk=p.pk).update(is_locked=False, pickled=pickle.dumps(process))

        if process.is_complete():
            Process.objects.filter(pk=p.pk).update(state=2)
            if p.is_subprocess:
                # TODO: callback as a component,  need a task_id
                pass

        map(lambda x: x.kill(), process._tasklets)
    else:
        print "#" * 40
        print "Got lock failed"
        print "#" * 40


@task()
def confirm(task_id):

    try:
        t = Task.objects.get(pk=task_id)
    except Task.DoesNotExist:
        pass
    else:
        if not t.is_confirmed:
            print "#" * 40
            print "Confirm work"
            print "#" * 40
            schedule.apply_async(args=(t.process_id,))
        else:
            print "#" * 40
            print "Confirm not work"
            print "#" * 40


@task()
def callback(task_id, result):

    try:
        t = Task.objects.get(pk=task_id)
    except Task.DoesNotExist:
        pass
    else:
        Task.objects.filter(pk=task_id)\
                       .update(is_complete=True,
                               result=cPickle.dumps(result))

        schedule.apply_async(args=(t.process_id,))
        confirm.apply_async(args=(t.id,), countdown=10)


@task()
def start(name, task_id=None, *args, **kwargs):

    try:
        defination = Defination.objects.get(name=name)
    except Defination.DoesNotExist:
        pass
    else:
        ns = {
            '__builtins__': None,
            '__name__': __name__,
            'BaseProcess': BaseProcess,
        }
        code = compile(defination.content, '<stdin>', 'exec')

        try:
            exec code in ns
        except:
            pass

        cls = ns[name]
        globals()[name] = cls

        p = cls(defination_id=defination.id)
        p.initiate(*args, **kwargs)
        Process.objects.filter(pk=p.process_id).update(pickled=cPickle.dumps(p))
        map(lambda x: x.kill(), p._tasklets)
        schedule.apply_async(args=(p.process_id,))

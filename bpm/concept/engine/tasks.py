# coding: utf-8
import cPickle
import pickle
import stackless

from celery import task

from concept.engine.models import Defination, Process, Task
from concept.engine.base import BaseTask
from django.db import transaction

import time


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
            'True': True,
            'False': False,
            'None': None,
            'BaseProcess': BaseProcess,
            'BaseComponent': BaseComponent,
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
            args = pickle.loads(str(t.args))
            kwargs = pickle.loads(str(t.kwargs))
        except:
            args = None
            kwargs = None
            pass    # TODO error log

        # cls.start(args, kwargs)
        print "begin to call celery start with name=%s, task=%s" % (name, task_id)
        start.delay(name, task_id, args, kwargs)


@task()
def schedule(process_id):
    """
    p1有子过程p2
    p1.task: task1 (id=1)      task1.process: p1
    p2.task: task2 (id=2)      task2.process: p1

    p2完成后, callback(2, result) -> schedule task2.process (schedule p1)
    in p1 schedule, p1.task.process==self, do not callback
    """
    try:
        p = Process.objects.get(pk=process_id)
    except Process.DoesNotExist:
        print "*" * 40
        print "FATAL ERROR, PROCESS DoesNotExist"
        print "*" * 40
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
            'True': True,
            'False': False,
            'None': None,
            'BaseProcess': BaseProcess,
            'join': 'join',
        }
        code = compile(p.defination.content, '<stdin>', 'exec')

        try:
            exec code in ns
        except:
            print "*" * 40
            print "FATAL ERROR, exec code in ns error"
            print "*" * 40
            pass

        globals()[p.defination.name] = ns[p.defination.name]
        print "$" * 40
        print "try to loads pickled"
        process = cPickle.loads(str(p.pickled))
        print "$" * 40
        print "try to resume"

        #if p.task.process == p:  # debug master process
        if p.task.process != p:   # debug subprocess
            import sys

            sys.path.extend(['D:\\develop\\BPM.py\\bpm',
                             'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pycharm',
                             'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pydev'])
            import pydevd
            #pydevd.settrace('localhost', port=8023, stdoutToServer=True, stderrToServer=True)
            pass  # for debug, to see subprocess status after stackless.schedule()

        process.resume()

        stackless.schedule()
        print "after schedule"

        while process.can_continue():
            print "#" * 40
            print "Process %d can continue" % process.process_id
            print "#" * 40
            stackless.schedule()

        Process.objects.filter(pk=p.pk).update(is_locked=False, pickled=pickle.dumps(process))

        if process.is_complete():
            Process.objects.filter(pk=p.pk).update(state=2)
            # subprocess callback
            if not p.task.process == p:  # subprocess
                print "subprocess callback"
                callback.delay(p.task.id, p.task.result)
            else:  # main process
                pass  # TODO: 可以做一些其他动作
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
    import sys

    sys.path.extend(['D:\\develop\\BPM.py\\bpm',
                     'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pycharm',
                     'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pydev'])
    import pydevd
    #pydevd.settrace('localhost', port=8024, stdoutToServer=True, stderrToServer=True)
    pass  # for debug
    try:
        t = Task.objects.get(pk=task_id)
    except Task.DoesNotExist:
        pass
    else:
        print "*" * 40
        print "ready to schedule process: %s" % t.process_id
        Task.objects.filter(pk=task_id) \
            .update(is_complete=True,
                    result=cPickle.dumps(result))
        schedule.apply_async(args=(t.process_id,))
        confirm.apply_async(args=(t.id,), countdown=10)


@task()
def start(name, task_id, *args, **kwargs):
    try:
        defination = Defination.objects.get(name=name)
    except Defination.DoesNotExist:
        pass    # TODO
    else:
        ns = {
            '__builtins__': None,
            '__name__': __name__,
            'True': True,
            'False': False,
            'None': None,
            'BaseProcess': BaseProcess,
            'BaseComponent': BaseComponent,
        }
        code = compile(defination.content, '<stdin>', 'exec')

        try:
            exec code in ns
        except:
            pass

        cls = ns[name]      # 可能不需要检查类型了，只需定义好统一接口
        globals()[name] = cls   # 添加到sys.modules 里是啥效果？

        # 实例化cls, task_id==None的情况只能是在new process中出现
        obj = cls()
        # 对于process, 是将self.start方法的tasklet添加到self._tasklets列表中
        # 对于component, 是执行self.start(暂定这个名字)中的逻辑代码
        # 因为start本身是通过启动器(新建过程实例时)或者TaskHandler调用celery的task_manager来执行的，
        # 因此这里就不需要再放到另外的celery中跑逻辑了
        print "#" * 40
        print "call init with def_id=%s, task_id=%s" % (defination.id, task_id)
        print "#" * 40

        obj.init(defination_id=defination.id, task_id=task_id)
        obj.run(*args, **kwargs)

        # 放到BaseProcess中去实现了:
        # Process.objects.filter(pk=p.process_id).update(pickled=cPickle.dumps(p))
        # map(lambda x: x.kill(), p._tasklets)
        # schedule.apply_async(args=(p.process_id,))


#

def join(*handlers):
    for handler in handlers:
        handler.blocked = 2
    for handler in handlers:
        handler.wait()


class TaskHandler(object):
    def __init__(self, cls, target, predecessors):
        if not self in cls._handlers:
            print "#" * 40
            print "Create handler for task %s" % target
            print "#" * 40
            self.blocked = 0  # 0: unread; 1: unblock; 2: block
            self.cls = cls
            self.target = target
            self.predecessors = predecessors

            self.cls.add_handler(self)

            task = Task(process=Process.objects.get(pk=cls.process_id))
            try:
                task.save()
            except:
                pass
            else:
                print "#" * 40
                print "Create task %d" % task.id
                print "#" * 40
                self.task_id = task.id

    def __call__(self, *args, **kwargs):
        print "$" * 40
        print "try to add handle to tasklet"
        self.cls.add_tasklet(stackless.tasklet(self.handle)(*args, **kwargs))
        return self

    def handle(self, *args, **kwargs):
        """
        """
        if self.predecessors:
            join(*self.predecessors)

        cleaned_args = []
        for arg in args:
            if isinstance(arg, self.__class__):
                arg = arg.read()    # 此时read不会阻塞了，因为predecessors都已经返回结果了
            cleaned_args.append(arg)

        cleaned_kwargs = {}
        for k, v in kwargs.iteritems():
            if isinstance(v, self.__class__):
                v = v.read()
            cleaned_kwargs[k] = v

        Task.objects.filter(pk=self.task_id).update(
            args=cPickle.dumps(cleaned_args),
            kwargs=cPickle.dumps(cleaned_kwargs),
        )

        # TODO: 实现任务管理器，进行实际的任务调用.
        # TODO: 需要处理requirement
        print
        print '#' * 40
        print 'Call %s: args=%s, kwargs=%s, task_id: %d' % (self.target, cleaned_args, cleaned_kwargs, self.task_id)
        print '#' * 40
        print
        task_manager.apply_async(args=(self.target, self.task_id))

    @transaction.commit_on_success()
    def instance(self):
        try:
            task = Task.objects.select_related().get(pk=self.task_id)
        except Task.DoesNotExist:
            pass
        else:
            return task

    def read(self):
        task = self.wait()
        # if task.result:
        #     return cPickle.loads(task.result)
        print "read result, result = %s" % str(task.result)
        return cPickle.loads(str(task.result))

    def wait(self):
        task = self.instance()

        import sys

        sys.path.extend(['D:\\develop\\BPM.py\\bpm',
                         'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pycharm',
                         'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pydev'])
        import pydevd
        #pydevd.settrace('localhost', port=8025, stdoutToServer=True, stderrToServer=True)
        pass  # for debug

        while not task.is_complete:
            # do something
            print "#" * 40
            print "Task %d blocked" % self.task_id
            print "#" * 40
            self.blocked = 2
            stackless.schedule()
            task = self.instance()

        Task.objects.filter(pk=self.task_id).update(is_confirmed=True)
        self.blocked = 1
        return task


class BaseComponent(BaseTask):
    """
        组件类需要继承此类
        并实现 start 方法
        如果组件调用的接口是异步的，建议使用辅助方法进行同步化
        辅助方法会提供多种轮询算法，特别对于审批、定时之类的耗时比较长的任务比较适用
    """
    enable_schedule = False
    algorithm = None

    def __init__(self, *args, **kwargs):
        super(BaseComponent, self).__init__(namespace='', *args, **kwargs)      # todo：namespace改为必填参数
        self.result = None
        self.task = None

    def init(self, defination_id, task_id):
        """组件和一个过程不同，不能直接执行，只能通过一个任务来启动"""
        if not task_id:
            return False, "need task_id"
        try:
            self.task = Task.objects.get(pk=task_id)
        except Task.DoesNotExist:
            return False, "task instance not exist， task_id=%s" % task_id

    def start(self, *args, **kwargs):
        """
        start方法需要组件开发者实现, 如cc接口调用， 如果没有设置self.enable_schedule=True，
        则返回的值会直接作为组件的结果进行 callback(celery task)
        """
        raise NotImplementedError

    def run(self, *args, **kwargs):
        print "before start"

        # import sys
        # sys.path.extend(['D:\\develop\\BPM.py\\bpm',
        #                  'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pycharm',
        #                  'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pydev'])
        # import pydevd
        # pydevd.settrace('localhost', port=8023, stdoutToServer=True, stderrToServer=True)
        # pass  # for debug

        self.result = self.start(*args, **kwargs)
        print "after start"
        if self.enable_schedule:
            self._schedule(*args, **kwargs)  # 在self._schedule里决定是否callback
        else:
            self._callback()

    def sync(self, *args, **kwargs):
        """
        组件开发者需要实现的方法, bool方法
        该方法发生在start方法的结果返回之后
        如果start方法是一个异步的请求，则可在sync方法中依据结果进行轮询查询
        sync方法返回true后就进行callback，反之则继续轮询

        要使用sync方法，需要设置 self.enable_schedule = True
        """
        raise NotImplementedError

    def _schedule(self, *args, **kwargs):
        """
            辅助方法
            对于异步调用的接口，可以通过实现sync方法，来实现异步任务的同步化
        """
        # TODO: 实现 algorithm
        while not self.sync(*args, **kwargs):
            time.sleep(5)
        self._callback()

    def _callback(self):
        """任务完成的通知"""
        print "%s callback with result = %s" % (self.__class__, str(self.result))
        self.task.result = self.result
        callback.delay(self.task.id, self.result)


class BaseProcess(BaseTask):
    def __init__(self, *args, **kwargs):
        super(BaseProcess, self).__init__(namespace='', *args, **kwargs)
        self.result = None
        self._handlers = []
        self._tasklets = []

    def init(self, defination_id, task_id):
        """
            对于BaseProcess, init方法负责创建任务实例及过程实例，若为子过程(task是事先创建了的)，则只创建过程实例

        """
        if task_id:  # subprocess
            print "#" * 40
            print "subprocess"
            print "#" * 40
            try:

                # import sys
                # sys.path.extend(['D:\\develop\\BPM.py\\bpm',
                #                  'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pycharm',
                #                  'D:\\Program Files\\JetBrains\\PyCharm 2.7.1\\helpers\\pydev'])
                # import pydevd
                # pydevd.settrace('localhost', port=8023, stdoutToServer=True, stderrToServer=True)
                # pass  # for debug, to see subprocess status after stackless.schedule()

                task = Task.objects.get(id=task_id)  # 这个task的process指向的是父过程
                process = Process(defination=Defination.objects.get(pk=defination_id), task=task)
                process.save()
                self.process_id = process.id
                print "create subprocess instance success"
            except Task.DoesNotExist:
                pass  # TODO: task_id对应的task不存在后的异常处理, 正常情况下不应该走到这个分支
        else:   # new process
            print "#" * 40
            print "new process, def_id = %s" % defination_id
            print "#" * 40
            process = Process(defination=Defination.objects.get(pk=defination_id))
            process.save()      # TODO: try一下?
            task = Task(process=process)
            task.save()  # 保存任务
            process.task = task
            process.save()      # TODO: 两次save :(  这里不知道有没有更好的处理方式
            self.process_id = process.id

    def add_handler(self, handler):
        if not handler in self._handlers:
            self._handlers.append(handler)

    def add_tasklet(self, tasklet):
        if not tasklet in self._tasklets:
            self._tasklets.append(tasklet)

    def can_continue(self):
        blocked_handlers = 0
        for handler in self._handlers:
            print "=" * 40
            if handler.blocked == 2:
                print "handler blocked +1"
                blocked_handlers += 1
                # if handler.blocked == 0:
                #     print "found an unread handler, count +1"
                #     count += 1
        alive_tasklet = 0
        for tasklet in self._tasklets[1:]:
            if tasklet.alive:
                alive_tasklet += 1
        print "#" * 40
        print "alive_tasklet: %d, handlers: %d, tasklets:%d, blocked_handlers: %d" % (
            alive_tasklet, len(self._handlers), len(self._tasklets), blocked_handlers)
        print "#" * 40

        if self._tasklets[0].alive:
            can_continue = alive_tasklet - 1 > blocked_handlers
        else:
            can_continue = alive_tasklet > blocked_handlers
        return can_continue   # or (len(self._tasklets) == 1 and run_count > 0)

    def _callback(self):
        """子过程任务完成后进行回调"""
        try:
            process = Process.objects.get(pk=self.process_id)
        except Process.DoesNotExist:
            return False  # TODO: error log, alert
        callback.delay(process.task.id, self.result)

    def terminate(self):
        """terminate方法被调用时，强制结束所有的tasklet， 并标记对应的Process实例状态为TERMINATED"""
        pass

    def pause(self):
        """pause方法被调用时，标记对应的Process实例状态为PAUSED"""
        pass

    def stop(self):
        """stop方法被调用时，强制结束所有的tasklet， 并标记对应的Process实例状态为COMPLETED"""
        pass

    def set_result(self, result):
        """
            在on_complete时被调用,
        """
        self.result = result

    def on_complete(self):
        """
        过程完成后进行的回调
        """
        pass

    def is_complete(self):
        run_count = 0
        for tasklet in self._tasklets:
            if tasklet.alive:
                run_count += 1

        if run_count:
            return False
        else:
            x = 0
            for handler in self._handlers:
                if handler.instance().is_complete:
                    x += 1
            if len(self._handlers) <= x:
                return True

    def run(self, *args, **kwargs):
        print "add %s to tasklet" % self.__class__
        self.add_tasklet(stackless.tasklet(self.start)(*args, **kwargs))
        Process.objects.filter(pk=self.process_id).update(pickled=cPickle.dumps(self))
        map(lambda x: x.kill(), self._tasklets)
        print "#" * 40
        print "%s schedule called" % self.__class__
        print "#" * 40
        schedule.apply_async(args=(self.process_id,))

    @transaction.commit_on_success()
    def instance(self):
        try:
            process = Process.objects.get(pk=self.process_id)
        except Process.DoesNotExist:
            pass
        else:
            return process

    def resume(self):
        # for t in self._tasklets:
        #     print "-" * 40
        #     if not t.alive:
        #         print "remove inactive tasklet"
        #         #self._tasklets.remove(t)
        #         pass
        for t in self._tasklets:
            try:
                print "insert tasklet"
                if t.alive:
                    t.insert()
            except:
                pass

    def start(self):
        raise NotImplementedError

    def tasklet(self, target, predecessors=[]):
        """
        作用及原理简述:
            流程开发者在重写start方法时，通过调用该方法，可以实现组件或子过程的调用
            该方法返回一个 任务句柄(TaskHandler)
            如果要取得一个任务的结果，通过 TaskHandler的read方法(该方法会产生阻塞)得到，适用于做一些条件判断
            在调用TaskHandler的__call__方法时，通过自身传递给TaskHandler，进而将任务的tasklet注册回自身的tasklet列表中

        参数说明:
            target: <str>  -- 欲执行的任务(组件或子过程)名

            predecessors: <list> -- [前置任务句柄, ....]
                        这个参数用于指定当前任务执行时需要的前置任务，
                        在执行到该任务时(注: 如果还没执行到该任务，也就无所谓任务的状态了),
                        只有当前置任务都完成后。该任务才会被执行，否则会一直处于block状态

                        这个参数的作用在于有些任务需要依赖之前任务的结果时(通过__call__方法,传递任务句柄得到)，
                        可以指定具体依赖哪些前置任务，任务在执行时，会等待这些前置任务的完成(调用join方法),从而避免阻塞

        """
        print "#" * 40
        print "Create tasklet for %s" % target
        print "#" * 40
        return TaskHandler(self, target, predecessors)

    def test(self):
        pass

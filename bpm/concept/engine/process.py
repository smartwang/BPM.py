# coding: utf-8
import cPickle
import stackless

from .base import BaseTask
from .models import Process, Task, Defination
from .tasks import start, schedule, task_manager, callback
import time


def join(*handlers):
    for handler in handlers:
        handler.wait()


class TaskHandler(object):
    def __init__(self, cls, target, predecessors):
        if not self in cls._handlers:
            print "#" * 40
            print "Create handler for task %s" % target
            print "#" * 40
            self.blocked = False
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
        #task_manager.apply_async(args=(self.target, self.task_id))

    def instance(self):
        try:
            task = Task.objects.get(pk=self.task_id)
        except Task.DoesNotExist:
            pass
        else:
            return task

    def read(self):
        task = self.wait()
        if task.result:
            return cPickle.loads(task.result)

    def wait(self):
        task = self.instance()
        while not task.is_complete:
            # do something
            print "#" * 40
            print "Task %d blocked" % self.task_id
            print "#" * 40
            self.blocked = True
            stackless.schedule()
            task = self.instance()

        Task.objects.filter(pk=self.task_id).update(is_confirmed=True)
        self.blocked = False
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
        self.result = self.start(*args, **kwargs)
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
            try:
                task = Task.objects.get(id=task_id)  # 这个task的process指向的是父过程
                process = Process(defination=Defination.objects.get(pk=defination_id, task=task))
                self.process_id = process.id
            except Task.DoesNotExist:
                pass  # TODO: task_id对应的task不存在后的异常处理, 正常情况下不应该走到这个分支
        else:   # new process
            process = Process(defination=Defination.objects.get(pk=defination_id))
            process.save()      # TODO: try一下?
            task = Task(process=Process.objects.get(pk=defination_id, process=process))
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
        count = 0
        for handler in self._handlers:
            if handler.blocked:
                count += 1
        run_count = 0
        for tasklet in self._tasklets:
            if tasklet.alive:
                run_count += 1
        print "#" * 40
        print "run_count: %d, handlers: %d, blocked_handlers: %d" % (run_count, len(self._handlers), count)
        print "#" * 40
        return run_count - 1 > count

    def _callback(self):
        """子过程任务完成后进行回调"""
        pass    # TODO: to be implemented

    def stop(self):
        """"""
        pass

    def get_result(self, null=True):
        """
        该方法可由过程开发者进行覆盖
        在callback时调用
        """
        if null:  # 不需要给别人结果的过程
            return None
        else:   # 需要给出一个结果的过程调用，通常子过程需要这个东西(当然也不是必须的)

            pass

    def on_complete(self):
        """
        过程完成后进行回调
        """
        self.get_result()
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
        self.add_tasklet(stackless.tasklet(self.start)(*args, **kwargs))
        Process.objects.filter(pk=self.process_id).update(pickled=cPickle.dumps(self))
        map(lambda x: x.kill(), self._tasklets)
        schedule.apply_async(args=(self.process_id,))

    def instance(self):
        try:
            process = Process.objects.get(pk=self.process_id)
        except Process.DoesNotExist:
            pass
        else:
            return process

    def resume(self):
        for t in self._tasklets:
            if not t.alive:
                self._tasklets.remove(t)

        for t in self._tasklets:
            try:
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

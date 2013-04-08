# coding: utf-8
import cPickle
import stackless

from .base import BaseTask
from .models import Process, Task, Defination
#from .tasks import start, schedule, task_manager, callback

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
        并实现 start/on_callback 方法
    """
    def __init__(self, defination_id, *args, **kwargs):
        super(BaseComponent, self).__init__(namespace='', *args, **kwargs)      # todo：namespace改为必填参数

    def start(self):
        """
            start方法需要组件开发者实现, 如果没有定义on_callback方法，则返回的值会直接作为组件的结果进行 callback(celery task)
        """
        raise NotImplementedError

    def on_callback(self):
        """"""
        return None

    def schedule(self, algorithm=None, *args, **kwargs):
        """
            辅助方法
            对于异步调用的接口，可以通过实现on_callback方法，来实现异步任务的同步化
        """
        raise NotImplementedError



class BaseProcess(BaseTask):

    def __init__(self, defination_id, *args, **kwargs):
        super(BaseProcess, self).__init__(namespace='', *args, **kwargs)

        self._handlers = []
        self._tasklets = []

        process = Process(defination=Defination.objects.get(pk=defination_id))
        try:
            process.save()
        except:
            pass
        else:
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

    def initiate(self, *args, **kwargs):
        self.add_tasklet(stackless.tasklet(self.start)(*args, **kwargs))

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

import sys

from .workflows import TestSignal, issue_refund

payment_id, amount = sys.argv[1], int(sys.argv[2])
env = sys.argv[3] if len(sys.argv) > 3 else 'task'

match env:
    case 'direct':
        print(issue_refund(payment_id, amount))
    case 'thread':
        import threading
        import time
        from lightemporal import ENV
        from lightemporal.runner import thread_runner_env
        from lightemporal.workflow import workflow

        with thread_runner_env():
            handler = issue_refund.start(payment_id, amount)
            parent_env = dict(ENV)
            stopped = False

            def setter():
                with ENV.new_layer():
                    ENV.update(parent_env)
                    for _ in range(2):
                        for _ in range(6):
                            if stopped:
                                break
                            time.sleep(1)
                        if stopped:
                            break
                        print('signal')
                        workflow.signal(handler.workflow_id, TestSignal(message='test'))

            print(handler)
            thr = threading.Thread(target=setter)
            thr.start()
            try:
                print(handler.result())
            finally:
                stopped = True
                thr.join()
    case 'task':
        from lightemporal.worker import runner_env

        with runner_env():
            handler = issue_refund.start(payment_id, amount)
            print(handler, vars(handler))
            print(handler.result())

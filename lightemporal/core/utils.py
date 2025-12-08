import inspect
import time
from contextlib import contextmanager
from functools import cached_property
from typing import Annotated
from uuid import uuid4

import pydantic


UUID = Annotated[str, pydantic.Field(default_factory=lambda: str(uuid4()))]


@contextmanager
def _repeat_context(loop):
    try:
        yield
    except loop.exc_type as e:
        if loop.error is None:
            loop.error = e


class repeat_if_needed:
    def __init__(self, *, exc_type=Exception, blocking=True, sleep_time=0.1, error=None):
        self.exc_type = exc_type
        self.blocking = blocking
        self.sleep_time = sleep_time
        self.error = error

    def __iter__(self):
        while True:
            yield _repeat_context(self)

            if self.blocking:
                time.sleep(self.sleep_time)
            else:
                raise self.error or RuntimeError()


class SignatureWrapper:
    def __init__(self, signature):
        self.signature = signature

    @classmethod
    def from_function(cls, function):
        return cls(inspect.signature(function))

    @cached_property
    def args_model(self):
        return tuple[*(
            p.annotation
            for p in self.signature.parameters.values()
            if p.kind is not p.KEYWORD_ONLY
        )]

    @cached_property
    def kwargs_model(self):
        return pydantic.create_model(
            'kwargs',
            **{
                p.name: p.annotation
                for p in self.signature.parameters.values()
                if p.kind is p.KEYWORD_ONLY
            }
        )

    @cached_property
    def input_adapter(self):
        return pydantic.TypeAdapter(tuple[self.args_model, self.kwargs_model])

    @cached_property
    def output_adapter(self):
        return pydantic.TypeAdapter(self.signature.return_annotation)

    def load_input(self, input: str):
        args, kwargs = self.input_adapter.validate_json(input)
        return args, kwargs.model_dump()

    def dump_input(self, *args, **kwargs) -> str:
        bound = self.signature.bind(*args, **kwargs)
        args, kwargs = bound.args, bound.kwargs
        kwargs = self.kwargs_model(**kwargs)
        return self.input_adapter.dump_json((args, kwargs)).decode()

    def load_output(self, output: str):
        return self.output_adapter.validate_json(output)

    def dump_output(self, value) -> str:
        return self.output_adapter.dump_json(value).decode()

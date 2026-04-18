import copy

class _GlobalArgsProxy:
    def __getattribute__(self, name):
        if name in ("__class__", "__repr__", "__getattribute__", "__setattr__", "__dict__"):
            return object.__getattribute__(self, name)
        return getattr(_global_args, name)

    def __setattr__(self, name, value):
        setattr(_global_args, name, value)

class Namespace:
    pass

_global_args = Namespace()
_global_args.workspace = "default"

proxy = _GlobalArgsProxy()
ws_args = copy.copy(proxy)

ws_args.workspace = "ws1"
print(f"ws_args.workspace: {ws_args.workspace}")
print(f"_global_args.workspace: {_global_args.workspace}")

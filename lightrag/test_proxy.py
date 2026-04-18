import copy

class Namespace:
    def __init__(self):
        self.workspace = "default"

_global_args = Namespace()
_initialized = True

class _GlobalArgsProxy:
    def __getattribute__(self, name):
        if name in ("__class__", "__repr__", "__getattribute__", "__setattr__", "__copy__"):
            return object.__getattribute__(self, name)
        return getattr(_global_args, name)

    def __setattr__(self, name, value):
        setattr(_global_args, name, value)

proxy = _GlobalArgsProxy()
ws_args = copy.copy(proxy)
ws_args.workspace = "new_workspace"

print("ws_args:", type(ws_args))
print("ws_args.workspace:", ws_args.workspace)
print("proxy.workspace:", proxy.workspace)
print("_global_args.workspace:", _global_args.workspace)

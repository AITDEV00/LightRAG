import copy

class _GlobalArgsProxy:
    def __getattribute__(self, name):
        if name == "__dict__":
            return {"workspace": "default"}
        if name in ("__class__", "__repr__", "__getattribute__", "__setattr__"):
            return object.__getattribute__(self, name)
        return object.__getattribute__(self, name) # simplified
    def __setattr__(self, name, value):
        print(f"Setting {name} to {value} on proxy!")

proxy = _GlobalArgsProxy()
copied = copy.copy(proxy)
print(type(copied))
print(copied.__class__)
copied.workspace = "new"

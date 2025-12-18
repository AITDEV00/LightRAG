from splink import DuckDBAPI, Linker, SettingsCreator
import splink.comparison_library as cl
import pandas as pd

try:
    c = cl.ExactMatch("entity_type")
    print("Attributes:", dir(c))
    if hasattr(c, "configure"):
        print("Has configure method")
    # Try to set params in constructor if possible, or via configure
    # print(c.to_dict())
except Exception as e:
    print(e)

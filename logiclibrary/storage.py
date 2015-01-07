import os
import importlib

if 'OPERATION_PRIMITIVES' in os.environ:
    import_name = os.environ['OPERATION_PRIMITIVES']
else:
    import_name = 'operation_primitives' #assume it has the standard name and is on the python path
OPERATION_PRIMITIVES = importlib.import_module(import_name)

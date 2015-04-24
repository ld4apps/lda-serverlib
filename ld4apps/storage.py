import os
import importlib

if 'OPERATION_PRIMITIVES' in os.environ:
    import_name = os.environ['OPERATION_PRIMITIVES']
else:
    import_name = 'ld4apps.mongodbstorage.operation_primitives' # Default use MongoDB storage
operation_primitives = importlib.import_module(import_name)

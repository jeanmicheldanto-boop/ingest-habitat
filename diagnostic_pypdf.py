import sys
import traceback

print('sys.executable=', sys.executable)
print('sys.version=', sys.version)
print('sys.path:')
for p in sys.path:
    print('  ', p)

try:
    import pypdf
    print('pypdf imported, version=', getattr(pypdf, '__version__', None))
except Exception:
    print('pypdf import failed:')
    traceback.print_exc()

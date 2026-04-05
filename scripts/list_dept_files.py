import glob
import sys

depts = sys.argv[1:] if len(sys.argv)>1 else ['78','91','92','93','94','95']
for d in depts:
    files = sorted(glob.glob(f'data/data_{d}_*'))
    print(f'--- dept {d} ---')
    if not files:
        print('  (no files)')
    for f in files:
        print('  ', f)

from obj.operating import create_orchestrator_by_dir
import sys

dir = sys.argv[1]

sys.argv.pop(0)
sys.argv.pop(0)

args = {sys.argv[i].lower(): sys.argv[i+1] for i in range(0, len(sys.argv), 2)} if sys.argv else {}

create_orchestrator_by_dir(dir=dir, **args).execute()
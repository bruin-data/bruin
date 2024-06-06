import json
import sys
from parser.main import get_column_lineage

def main():
    while True:
        cmd = sys.stdin.readline()
        if not cmd:
            break

        cmd = json.loads(cmd)

        result = {}
        if cmd["command"] == "init":
            pass
        elif cmd["command"] == "lineage":
            c = cmd["contents"]
            result = get_column_lineage(c["query"], c["schema"], c["dialect"])
            pass

        elif cmd["command"] == "exit":
            break
        else:
            raise Exception("invalid cmd")

        result = json.dumps(result)

        sys.stdout.write(result + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        pass
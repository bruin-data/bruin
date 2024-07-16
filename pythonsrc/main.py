import json
import sys
import logging
from parser.main import get_column_lineage, get_tables

logging.basicConfig(filename="/Users/burak/Code/personal/bruin/internal/bruin-cli/pylog.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

def main():
    while True:
        logging.info("running loop")
        cmd = sys.stdin.readline()
        if not cmd:
            break

        cmd = json.loads(cmd)
        logging.info("-- loaded json", cmd)

        result = {}
        if cmd["command"] == "init":
            pass
        elif cmd["command"] == "lineage":
            c = cmd["contents"]
            result = get_column_lineage(c["query"], c["schema"], c["dialect"])
            pass
        elif cmd["command"] == "get-tables":
            c = cmd["contents"]
            result = get_tables(c["query"], c["dialect"])
            pass

        elif cmd["command"] == "exit":
            break
        else:
            raise Exception("invalid cmd")

        logging.info("-- returning json", result)

        result = json.dumps(result)

        sys.stdout.write(result + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        pass
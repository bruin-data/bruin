import json
import sys
import logging
import os
from parser.main import get_column_lineage, get_tables

from pathlib import Path

home = str(Path.home())
log_dir = f"{home}/.bruin/pylogs"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    filename=f"{log_dir}/parser_debug.log",
    filemode="a",
    format="%(asctime)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    level=logging.DEBUG,
)


def main():
    logging.info("starting the loop")
    while True:
        try:
            logging.info("running loop")
            cmd = sys.stdin.readline()
            if not cmd:
                break
            raw_cmd = cmd
            logging.info("loaded json: " + raw_cmd)
            cmd = json.loads(cmd)

            result = {}
            if cmd["command"] == "init":
                logging.info("got init command")
                pass
            elif cmd["command"] == "lineage":
                logging.info("got lineage command")
                c = cmd["contents"]
                result = get_column_lineage(c["query"], c["schema"], c["dialect"])
            elif cmd["command"] == "get-tables":
                logging.info("got get-tables command")
                c = cmd["contents"]
                result = get_tables(c["query"], c["dialect"])
            elif cmd["command"] == "replace-table-references":
                from parser.rename import replace_table_references

                logging.info("got replace-table-references command")
                c = cmd["contents"]
                result = replace_table_references(
                    c["query"], c["dialect"], c["table_mapping"]
                )
            elif cmd["command"] == "exit":
                logging.info("got exit command amx")
                break
            else:
                logging.info("invalid cmd arrived: " + raw_cmd)
                raise Exception("invalid cmd")

            result = json.dumps(result)
            logging.info("-- returning json: " + result)

            sys.stdout.write(result + "\n")
            sys.stdout.flush()
        except Exception as e:
            logging.info("exception occured", str(e))
            sys.stdout.write(json.dumps({"error": str(e)}) + "\n")
            sys.stdout.flush()

    logging.info("got out of the loop")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass

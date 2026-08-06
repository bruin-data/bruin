import json
import logging
import os
import sys
from pathlib import Path

from parser.main import (
    add_ctes,
    add_limit,
    extract_select,
    freeze_time,
    get_column_lineage,
    get_tables,
    is_single_select_query,
    select_cte,
)

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
            elif cmd["command"] == "add-limit":
                logging.info("got add-limit command")
                c = cmd["contents"]
                result = add_limit(c["query"], c["limit"], c["dialect"])
            elif cmd["command"] == "is-single-select":
                logging.info("got is-single-select command")
                c = cmd["contents"]
                result = is_single_select_query(c["query"], c["dialect"])
            elif cmd["command"] == "add-ctes":
                logging.info("got add-ctes command")
                c = cmd["contents"]
                result = add_ctes(c["query"], c.get("dialect"), c.get("ctes"))
            elif cmd["command"] == "extract-select":
                logging.info("got extract-select command")
                c = cmd["contents"]
                result = extract_select(c["query"], c.get("dialect"))
            elif cmd["command"] == "select-cte":
                logging.info("got select-cte command")
                c = cmd["contents"]
                result = select_cte(c["query"], c.get("dialect"), c.get("cte_name"))
            elif cmd["command"] == "freeze-time":
                logging.info("got freeze-time command")
                c = cmd["contents"]
                result = freeze_time(
                    c["query"], c.get("dialect"), c.get("execution_time")
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

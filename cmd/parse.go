package cmd

import (
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/urfave/cli/v2"
)

func Parse() *cli.Command {
	return &cli.Command{
		Name:      "parse",
		Usage:     "parse a single Bruin SQL asset",
		ArgsUsage: "[path to the asset definition]",
		Action: func(c *cli.Context) error {
			tables, err := sqlparser.ParseUsedTables(`
            with t1 as (
            select *
            from table1
        ),
        t2 as (
            select *
            from table2
        )
        select *
        from t1
        join t2
            using(a)`)

			if err != nil {
				panic(err)
			}

			for _, table := range tables {
				println(table)
			}

			return nil
		},
	}
}

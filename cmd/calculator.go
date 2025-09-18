package cmd

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bruin-data/bruin/pkg/dummy"
	"github.com/urfave/cli/v3"
)

// CalculatorCmd returns a CLI command for testing the dummy calculator package
func CalculatorCmd() *cli.Command {
	return &cli.Command{
		Name:  "calculator",
		Usage: "Test calculator functions",
		Action: func(ctx context.Context, c *cli.Command) error {
			operation := c.String("operation")
			aStr := c.String("a")
			bStr := c.String("b")

			// Parse the numbers
			a, err := strconv.Atoi(aStr)
			if err != nil {
				return fmt.Errorf("invalid number a: %v", err)
			}

			b, err := strconv.Atoi(bStr)
			if err != nil {
				return fmt.Errorf("invalid number b: %v", err)
			}

			// Perform the operation
			switch operation {
			case "add":
				result := dummy.Add(a, b)
				fmt.Printf("%d + %d = %d\n", a, b, result)
			case "multiply":
				result := dummy.Multiply(a, b)
				fmt.Printf("%d * %d = %d\n", a, b, result)
			default:
				return fmt.Errorf("unsupported operation: %s", operation)
			}

			return nil
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "operation",
				Usage: "Operation to perform (add or multiply)",
				Value: "add",
			},
			&cli.StringFlag{
				Name:  "a",
				Usage: "First number",
				Value: "5",
			},
			&cli.StringFlag{
				Name:  "b",
				Usage: "Second number",
				Value: "3",
			},
		},
	}
}

# pgbox-go

Official Go client SDK for **PGB Hub**.

## Installation

```bash
go get github.com/PGBHub/pgbox-sdk-go
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/PGBHub/pgbox-sdk-go"
)

func main() {
	client := pgbox.NewClient(pgbox.Config{
		APIKey:   "your_api_key",
		Endpoint: "https://console.pgbhub.com",
		Database: "my_database",
	})

	ctx := context.Background()

	// Invoke a declarative action
	res, err := client.InvokeAction(ctx, "approveOrder", map[string]any{
		"orderId": "ord_102",
		"userId":  "usr_99",
	}, false)
	if err != nil {
		log.Fatalf("Action failed: %v", err)
	}

	fmt.Printf("Status: %s (took %.2f ms)\n", res.Status, res.DurationMS)
	fmt.Printf("Results: %s\n", string(res.Results))
}
```

## License

MIT © [PGBHub](https://github.com/PGBHub)

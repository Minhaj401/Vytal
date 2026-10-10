// Command vytal-db ensures Postgres tables exist.
package main

import (
	"fmt"
	"os"

	"github.com/Minhaj401/Vytal/internal/config"
	"github.com/Minhaj401/Vytal/internal/store"
)

func main() {
	cfg := config.Load("")
	if err := store.Ensure(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println("tables ok")
}

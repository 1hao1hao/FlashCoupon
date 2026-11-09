package main

import (
	"flashcoupon/internal/app"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: token USER_ID (USER_KEY env)")
		os.Exit(1)
	}
	u, e := strconv.ParseInt(os.Args[1], 10, 64)
	if e != nil || u <= 0 {
		os.Exit(1)
	}
	key := os.Getenv("USER_KEY")
	if key == "" {
		key = "dev-user-key"
	}
	fmt.Println(app.SignToken(u, time.Now().Add(time.Hour).Unix(), key))
}

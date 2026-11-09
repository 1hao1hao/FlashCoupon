package main

import (
	"fmt"
	"github.com/xuri/excelize/v2"
	"os"
)

func main() {
	path := "reports/whitelist.xlsx"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	f := excelize.NewFile()
	defer f.Close()
	w, e := f.NewStreamWriter("Sheet1")
	if e != nil {
		panic(e)
	}
	w.SetRow("A1", []interface{}{"user_id"})
	for i := 1; i <= 100; i++ {
		if e = w.SetRow(fmt.Sprintf("A%d", i+1), []interface{}{100000 + i}); e != nil {
			panic(e)
		}
	}
	if e = w.Flush(); e != nil {
		panic(e)
	}
	if e = f.SaveAs(path); e != nil {
		panic(e)
	}
}

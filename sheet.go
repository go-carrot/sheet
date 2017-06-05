package sheet

import (
	"encoding/csv"
	"fmt"
	"os"
)

type Handler func(params []string, data *map[string]interface{}) error

type Operation struct {
	Columns []int
	Handler Handler
}

type Row struct {
	Data       map[string]interface{}
	Operations []Operation
}

type CSV struct {
	FilePath   string
	IgnoreRows []int
	Row        Row
}

func Consume(csvDefinition CSV) error {
	// Open the CSV file for reading
	file, err := os.Open(csvDefinition.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// New CSV Reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	// Handle each record
	for i, record := range records {
		if !rowIgnored(i, csvDefinition.IgnoreRows) {
			handleRecord(i, record, csvDefinition.Row)
		}
	}

	// OK
	return nil
}

func rowIgnored(rowNumber int, ignoredRows []int) bool {
	for _, ignoredRow := range ignoredRows {
		if ignoredRow == rowNumber {
			return true
		}
	}
	return false
}

func handleRecord(index int, record []string, row Row) {
	row.Data = make(map[string]interface{})
	for _, model := range row.Operations {
		var params = []string{}
		for _, columnNumber := range model.Columns {
			params = append(params, record[columnNumber])
		}
		err := model.Handler(params, &row.Data)
		if err != nil {
			// If there's an error, break out
			fmt.Printf("Skipping row %v\nError: %v\n", index, err)
			return
		}
	}
}

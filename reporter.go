package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"

	"github.com/alexeyco/simpletable"
	yaml "gopkg.in/yaml.v2"
)

// Reporter generates the report from the given slice of BenchmarkResult to a string format
type Reporter interface {
	ReportResults(br []*BenchmarkResult) string
}

type JsonReporter struct {
}

var _ Reporter = JsonReporter{}

func (jr JsonReporter) ReportResults(br []*BenchmarkResult) string {
	j, _ := json.MarshalIndent(br, "", "  ")
	return string(j)
}

type YamlReporter struct {
}

var _ Reporter = YamlReporter{}

func (yr YamlReporter) ReportResults(br []*BenchmarkResult) string {
	y, _ := yaml.Marshal(br)
	return string(y)
}

type CsvReporter struct {
}

var _ Reporter = CsvReporter{}

func (cr CsvReporter) ReportResults(brs []*BenchmarkResult) string {

	buffer := bytes.NewBuffer(make([]byte, 0))
	csvWr := csv.NewWriter(buffer)
	header := []string{"Test", "QPS", "Min", "Avg", "Median", "P75", "P90", "P99", "Max"}
	csvWr.Write(header)

	for _, br := range brs {
		data := make([]string, 0, 9)
		data = append(data, br.BenchTestName)
		data = append(data, cr.valueToString(br.QPS))
		data = append(data, cr.valueToString(br.MinLatency))
		data = append(data, cr.valueToString(br.AvgLatency))
		data = append(data, cr.valueToString(br.MedianLatency))
		data = append(data, cr.valueToString(br.P75Latency))
		data = append(data, cr.valueToString(br.P90Latency))
		data = append(data, cr.valueToString(br.P99Latency))
		data = append(data, cr.valueToString(br.MaxLatency))
		csvWr.Write(data)
	}
	return string(buffer.Bytes())
}

func (cr CsvReporter) valueToString(f float64) string {
	if f == 0.0 {
		return "NA"
	}
	return fmt.Sprintf("%0.3f", f)
}

type TableReporter struct{}

var _ Reporter = TableReporter{}

func (tr TableReporter) ReportResults(brs []*BenchmarkResult) string {

	table := simpletable.New()

	hdrStr := []string{"Test", "QPS", "Min", "Avg", "Median", "P75", "P90", "P99", "Max"}
	header := make([]*simpletable.Cell, len(hdrStr))
	for i, h := range hdrStr {
		header[i] = &simpletable.Cell{Text: h}
	}
	table.Header = &simpletable.Header{
		Cells: header,
	}
	for _, br := range brs {
		row := make([]*simpletable.Cell, 0, 9)
		row = append(row, &simpletable.Cell{Text: br.BenchTestName})
		row = append(row, tr.valueToCell(br.QPS))
		row = append(row, tr.valueToCell(br.MinLatency))
		row = append(row, tr.valueToCell(br.AvgLatency))
		row = append(row, tr.valueToCell(br.MedianLatency))
		row = append(row, tr.valueToCell(br.P75Latency))
		row = append(row, tr.valueToCell(br.P90Latency))
		row = append(row, tr.valueToCell(br.P99Latency))
		row = append(row, tr.valueToCell(br.MaxLatency))
		table.Body.Cells = append(table.Body.Cells, row)
	}
	table.SetStyle(simpletable.StyleUnicode)
	return table.String()
}

func (tr TableReporter) valueToCell(f float64) *simpletable.Cell {
	if f == 0.0 {
		return &simpletable.Cell{Text: "NA"}
	}
	return &simpletable.Cell{Text: fmt.Sprintf("%0.3f", f), Align: simpletable.AlignRight}
}

func getResults(benchmarks []*Benchmark) []*BenchmarkResult {
	br := make([]*BenchmarkResult, 0, len(benchmarks))
	for _, b := range benchmarks {
		br = append(br, b.BenchmarkResult)
	}
	return br
}

func getReporter(format string) Reporter {
	switch format {
	case "json":
		return JsonReporter{}
	case "yaml":
		return YamlReporter{}
	case "csv":
		return CsvReporter{}
	case "table":
		return TableReporter{}
	default:
		return TableReporter{}
	}
}

package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

type Logger struct {
	*Config
	io.WriteCloser
}

func NewLogger(conf *Config, writer io.WriteCloser) *Logger {
	logger := Logger{
		Config:      conf,
		WriteCloser: writer,
	}
	return &logger
}

func (l *Logger) Debugf(f string, a ...interface{}) {
	if !l.Debug {
		return
	}
	f = "[DEBUG] " + l.ensureNewLine(f)
	l.Infof(f, a...)
}

func (l *Logger) Infof(f string, a ...interface{}) {
	if l.Quiet {
		return
	}
	f = l.ensureNewLine(f)
	fmt.Fprintf(l.WriteCloser, f, a...)
}

func (l *Logger) Errorf(f string, a ...interface{}) {
	f = "[ERROR] " + l.ensureNewLine(f)
	fmt.Fprintf(l.WriteCloser, f, a...)
}

func (l *Logger) Fatalf(f string, a ...interface{}) {
	f = "[FATAL] " + l.ensureNewLine(f)
	fmt.Fprintf(l.WriteCloser, f, a...)
	os.Exit(2)
}

func (l *Logger) ensureNewLine(f string) string {
	if !strings.HasSuffix(f, "\n") {
		f = f + "\n"
	}
	return f
}

func (l *Logger) Close() {
	l.WriteCloser.Close()
}

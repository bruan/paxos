package main

type statemachine interface {
	exec(value string) string
}

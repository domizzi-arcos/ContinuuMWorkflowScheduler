package main

import (
	"reflect"
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/algos"
)

func TestSelectPolicies(t *testing.T) {
	tests := []struct {
		name    string
		single  string
		list    string
		want    []string
		wantErr bool
	}{
		{name: "default is all registered", want: algos.Names()},
		{name: "single", single: "topsis", want: []string{"topsis"}},
		{name: "list keeps order", list: "topsis, random,best_fit", want: []string{"topsis", "random", "best_fit"}},
		{name: "both flags", single: "topsis", list: "random", wantErr: true},
		{name: "unknown single", single: "heft", wantErr: true},
		{name: "unknown in list", list: "random,nope", wantErr: true},
		{name: "duplicate", list: "random,random", wantErr: true},
		{name: "empty list", list: " , ", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := selectPolicies(tc.single, tc.list)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRunRejectsMissingConfig(t *testing.T) {
	if err := run([]string{"-duration", "1"}); err == nil {
		t.Fatal("want error when -config is missing")
	}
}

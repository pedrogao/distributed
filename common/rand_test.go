package common

import (
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestRandSeq(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "length: 10",
			args: args{
				n: 10,
			},
			want: 10,
		},
		{
			name: "length: 3",
			args: args{
				n: 3,
			},
			want: 3,
		},
		{
			name: "length: 100",
			args: args{
				n: 100,
			},
			want: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RandSeq(tt.args.n); len(got) != tt.want {
				t.Errorf("RandSeq()'s length = %v, want %v", got, tt.want)
			}
		})
	}
}

/**
goos: darwin
goarch: amd64
pkg: github.com/pedrogao/common
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkRandUInt64
BenchmarkRandUInt64-12    	 1000000	      1025 ns/op
*/
func BenchmarkRandUInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = RandUInt64()
	}
}

/**
goos: darwin
goarch: amd64
pkg: github.com/pedrogao/common
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkRandInt64
BenchmarkRandInt64-12    	 1060045	      1057 ns/op
PASS
*/
func BenchmarkRandInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = RandInt64()
	}
}

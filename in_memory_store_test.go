// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package taskqueue

import (
	"fmt"
	"testing"
)

func TestInMemoryStoreNew(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, len(st.dead); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestInMemoryStoreEnqueue(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	spec := &TaskSpec{ID: "1"}
	err := st.Enqueue(spec)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	spec = &TaskSpec{ID: "2"}
	err = st.Enqueue(spec)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 2, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := "1", st.input[0].ID; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := "2", st.input[1].ID; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestInMemoryStoreNextEmpty(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	spec, err := st.Next()
	if err != nil {
		t.Fatal(err)
	}
	if spec != nil {
		t.Fatalf("want %v, got %v", nil, err)
	}
}

func TestInMemoryStoreNextOneAvailable(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	in := &TaskSpec{ID: "1"}
	err := st.Enqueue(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := st.Next()
	if err != nil {
		t.Fatal(err)
	}
	if in != out {
		t.Fatalf("want %v, got %v", in, out)
	}
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	size, err := st.SizeOfWorkQueue()
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, size; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestInMemoryStoreNextManyAvailable(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	in := &TaskSpec{ID: "1"}
	err := st.Enqueue(in)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Enqueue(&TaskSpec{ID: "2"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := st.Next()
	if err != nil {
		t.Fatal(err)
	}
	if in != out {
		t.Fatalf("want %v, got %v", in, out)
	}
	if want, got := 1, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	size, err := st.SizeOfWorkQueue()
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 1, size; want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestInMemoryStoreDequeue(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	var specs []*TaskSpec
	const N = 5
	for i := 0; i < N; i++ {
		spec := &TaskSpec{ID: fmt.Sprintf("%d", i)}
		err := st.Enqueue(spec)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, spec)
	}
	if want, got := N, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	for i := 0; i < N; i++ {
		_, err := st.Next()
		if err != nil {
			t.Fatal(err)
		}
	}
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	err := st.Dequeue(specs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := N-1, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	err = st.Dequeue(specs[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if want, got := N-2, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestInMemoryStoreRetry(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	const N = 5
	for i := 0; i < N; i++ {
		st.work = append(st.work, &TaskSpec{ID: fmt.Sprintf("%d", i)})
	}
	if want, got := N, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	err := st.Retry(&TaskSpec{ID: "3"})
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 4, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, len(st.input); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

func TestInMemoryStoreMoveToDeadQueue(t *testing.T) {
	st := NewInMemoryStore()
	if want, got := 0, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 0, len(st.dead); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	const N = 5
	for i := 0; i < N; i++ {
		st.work = append(st.work, &TaskSpec{ID: fmt.Sprintf("%d", i)})
	}
	if want, got := N, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	err := st.MoveToDeadQueue(&TaskSpec{ID: "3"})
	if err != nil {
		t.Fatal(err)
	}
	if want, got := 4, len(st.work); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
	if want, got := 1, len(st.dead); want != got {
		t.Errorf("want %d, got %d", want, got)
	}
}

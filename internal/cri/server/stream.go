/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package server

import "context"

const defaultStreamBatchSize = 5000
const defaultStreamBatchMaxBytes = 15 << 20 // 15 MiB, with 1 MiB headroom below the 16 MiB gRPC limit

// sendInBatches sends items in batches using the provided send function.
// It checks for context cancellation before sending each batch.
// Returns nil immediately if items is empty (gRPC closes the stream with EOF).
func sendInBatches[T any](ctx context.Context, items []*T, batchSize int, sendFn func([]*T) error) error {
	for i := 0; i < len(items); i += batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(i+batchSize, len(items))
		if err := sendFn(items[i:end]); err != nil {
			return err
		}
	}
	return nil
}

// sendInBatchesBySize sends items in batches where each batch's total size
// (as reported by sizeFn) stays at or below maxBytes.
// If a single item exceeds maxBytes, it is sent alone in its own batch.
func sendInBatchesBySize[T any](ctx context.Context, items []*T, maxBytes int, sizeFn func(*T) int, sendFn func([]*T) error) error {
	batchStart := 0
	batchBytes := 0
	for i, item := range items {
		itemSize := sizeFn(item)
		if batchBytes+itemSize > maxBytes && batchStart < i {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := sendFn(items[batchStart:i]); err != nil {
				return err
			}
			batchStart = i
			batchBytes = 0
		}
		batchBytes += itemSize
	}
	if batchStart < len(items) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return sendFn(items[batchStart:])
	}
	return nil
}

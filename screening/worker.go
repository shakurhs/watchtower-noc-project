package screening

import (
	"context"
	"fmt"
	"sync"

	"watchtower/config"
	"watchtower/models"
)

func StartWorkerPool(
	ctx context.Context, 
	cfg *config.Config, 
	ingestionChannel <-chan models.EventEnvelope, 
	screenedChannel chan<- models.EventEnvelope, // Channel baru untuk data bersih
	wg *sync.WaitGroup,
) {
	processor := NewProcessor(cfg) 

	for i := 1; i <= cfg.Screening.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			fmt.Printf("[Worker %d] Aktif.\n", workerID)

			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-ingestionChannel:
					if !ok {
						return
					}

					if processor.IsDuplicate(event.ID) || processor.FilterNoise(event) {
						continue
					}

					screenedChannel <- event
				}
			}
		}(i)
	}
}
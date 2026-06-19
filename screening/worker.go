package screening

import (
	"context"
	"fmt"
	"sync"
	"log"

	"watchtower/config"
	"watchtower/models"
	"watchtower/policy"
)

func StartWorkerPool(
	ctx context.Context, 
	cfg *config.Config, 
	engine *policy.Engine,
	processor *Processor, 
	ingestionChannel <-chan models.EventEnvelope, 
	screenedChannel chan<- models.EventEnvelope,
	wg *sync.WaitGroup,
) {

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

					if processor.IsDuplicate(event.ID) || processor.FilterNoise(event) || processor.IsNoisy(event) {
						continue
					}

					priority := processor.ClassifyPriority(event)
					event.Priority = priority
					log.Printf("[DEBUG Worker] Event ID: %s, Source: %s, Priority: %s", event.ID, event.Source, priority)
					screenedChannel <- event
				}
			}
		}(i)
	}
}
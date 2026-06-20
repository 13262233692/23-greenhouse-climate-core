package database

import (
	"greenhouse-climate-core/internal/domain/entity"
	"greenhouse-climate-core/internal/domain/repository"
	"sync"
	"time"
)

type InMemoryPLCCommandRepository struct {
	commands  map[uint64]*entity.PLCCommand
	pending   []*entity.PLCCommand
	mu        sync.RWMutex
	idCounter uint64
}

func NewInMemoryPLCCommandRepository() repository.PLCCommandRepository {
	return &InMemoryPLCCommandRepository{
		commands:  make(map[uint64]*entity.PLCCommand),
		pending:   make([]*entity.PLCCommand, 0),
		idCounter: 1,
	}
}

func (r *InMemoryPLCCommandRepository) Save(command *entity.PLCCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if command.ID == 0 {
		command.ID = r.idCounter
		r.idCounter++
	}

	r.commands[command.ID] = command

	if command.Status == entity.PLCCommandStatusPending {
		r.pending = append(r.pending, command)
	}

	return nil
}

func (r *InMemoryPLCCommandRepository) FindByID(id uint64) (*entity.PLCCommand, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cmd, exists := r.commands[id]
	if !exists {
		return nil, nil
	}
	return cmd, nil
}

func (r *InMemoryPLCCommandRepository) FindPending() ([]*entity.PLCCommand, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*entity.PLCCommand, len(r.pending))
	copy(result, r.pending)
	return result, nil
}

func (r *InMemoryPLCCommandRepository) FindByGreenhouseID(greenhouseID string, from, to time.Time) ([]*entity.PLCCommand, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*entity.PLCCommand
	for _, cmd := range r.commands {
		if cmd.GreenhouseID == greenhouseID &&
			(cmd.CreatedAt.Equal(from) || cmd.CreatedAt.After(from)) &&
			(cmd.CreatedAt.Equal(to) || cmd.CreatedAt.Before(to)) {
			result = append(result, cmd)
		}
	}
	return result, nil
}

func (r *InMemoryPLCCommandRepository) UpdateStatus(command *entity.PLCCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.commands[command.ID] = command

	if command.Status != entity.PLCCommandStatusPending {
		for i, cmd := range r.pending {
			if cmd.ID == command.ID {
				r.pending = append(r.pending[:i], r.pending[i+1:]...)
				break
			}
		}
	}

	return nil
}

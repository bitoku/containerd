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

package v2

import (
	"context"
	"fmt"
	"os"

	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/events/exchange"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/pkg/timeout"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/runtime"
	"github.com/containerd/containerd/runtime/v2/task"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// Config for the v2 runtime
type Config struct {
	// Supported platforms
	Platforms []string `toml:"platforms"`
	// SchedCore enabled linux core scheduling
	SchedCore bool `toml:"sched_core"`
}

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.RuntimeShimPlugin,
		ID:   "shim",
		Requires: []plugin.Type{
			plugin.EventPlugin,
			plugin.MetadataPlugin,
		},
		Config: &Config{
			Platforms: defaultPlatforms(),
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			config := ic.Config.(*Config)
			supportedPlatforms, err := parsePlatforms(config.Platforms)
			if err != nil {
				return nil, err
			}

			ic.Meta.Platforms = supportedPlatforms

			m, err := ic.Get(plugin.MetadataPlugin)
			if err != nil {
				return nil, err
			}
			ep, err := ic.GetByID(plugin.EventPlugin, "exchange")
			if err != nil {
				return nil, err
			}
			cs := metadata.NewContainerStore(m.(*metadata.DB))
			events := ep.(*exchange.Exchange)

			return NewShimManager(ic.Context, &ManagerConfig{
				Root:         ic.Root,
				State:        ic.State,
				Address:      ic.Address,
				TTRPCAddress: ic.TTRPCAddress,
				Events:       events,
				Store:        cs,
				SchedCore:    config.SchedCore,
			})
		},
	})

	plugin.Register(&plugin.Registration{
		Type: plugin.RuntimePluginV2,
		ID:   "task",
		Requires: []plugin.Type{
			plugin.RuntimeShimPlugin,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			shimManagerInterface, err := ic.Get(plugin.RuntimeShimPlugin)
			if err != nil {
				return nil, err
			}

			shimManager := shimManagerInterface.(*ShimManager)

			// From now on task manager works via shim manager, which has different home directory.
			// Check if there are any leftovers from previous containerd versions and migrate home directory,
			// so we can properly restore existing tasks as well.
			if err := migrateTasks(ic, shimManager); err != nil {
				log.G(ic.Context).WithError(err).Error("unable to migrate tasks")
			}

			return NewTaskManager(shimManager), nil
		},
	})
}

func migrateTasks(ic *plugin.InitContext, shimManager *ShimManager) error {
	if !shimManager.list.IsEmpty() {
		return nil
	}

	// Rename below will fail is target directory exists.
	// `Root` and `State` dirs expected to be empty at this point (we check that there are no shims loaded above).
	// If for some they are not empty, these remove calls will fail (`os.Remove` requires a dir to be empty to succeed).
	if err := os.Remove(shimManager.root); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := os.Remove(shimManager.state); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(ic.Root, shimManager.root); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to migrate task `root` directory")
	}

	if err := os.Rename(ic.State, shimManager.state); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to migrate task `state` directory")
	}

	if err := shimManager.loadExistingTasks(ic.Context); err != nil {
		return fmt.Errorf("failed to load tasks after migration")
	}

	return nil
}

type ManagerConfig struct {
	Root         string
	State        string
	Store        containers.Store
	Events       *exchange.Exchange
	Address      string
	TTRPCAddress string
	SchedCore    bool
}

// NewShimManager creates a manager for v2 shims
func NewShimManager(ctx context.Context, config *ManagerConfig) (*ShimManager, error) {
	for _, d := range []string{config.Root, config.State} {
		if err := os.MkdirAll(d, 0711); err != nil {
			return nil, err
		}
	}

	m := &ShimManager{
		root:                   config.Root,
		state:                  config.State,
		containerdAddress:      config.Address,
		containerdTTRPCAddress: config.TTRPCAddress,
		list:                   runtime.NewTaskList(),
		events:                 config.Events,
		containers:             config.Store,
		schedCore:              config.SchedCore,
	}

	if err := m.loadExistingTasks(ctx); err != nil {
		return nil, err
	}

	return m, nil
}

// ShimManager manages currently running shim processes.
// It is mainly responsible for launching new shims and for proper shutdown and cleanup of existing instances.
// The manager is unaware of the underlying services shim provides and lets higher level services consume them,
// but don't care about lifecycle management.
type ShimManager struct {
	root                   string
	state                  string
	containerdAddress      string
	containerdTTRPCAddress string
	schedCore              bool
	list                   *runtime.TaskList
	events                 *exchange.Exchange
	containers             containers.Store
}

// ID of the shim manager
func (m *ShimManager) ID() string {
	return fmt.Sprintf("%s.%s", plugin.RuntimeShimPlugin, "shim")
}

// Start launches a new shim instance
func (m *ShimManager) Start(ctx context.Context, id string, opts runtime.CreateOpts) (_ ShimProcess, retErr error) {
	bundle, err := NewBundle(ctx, m.root, m.state, id, opts.Spec.Value)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			bundle.Delete()
		}
	}()

	shim, err := m.startShim(ctx, bundle, id, opts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			m.cleanupShim(shim)
		}
	}()

	// NOTE: temporarily keep this wrapper around until containerd's task service depends on it.
	// This will no longer be required once we migrate to client side task management.
	shimTask := &shimTask{
		shim: shim,
		task: task.NewTaskClient(shim.client),
	}

	if err := m.list.Add(ctx, shimTask); err != nil {
		return nil, errors.Wrap(err, "failed to add task")
	}

	return shimTask, nil
}

func (m *ShimManager) startShim(ctx context.Context, bundle *Bundle, id string, opts runtime.CreateOpts) (*shim, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}

	topts := opts.TaskOptions
	if topts == nil {
		topts = opts.RuntimeOptions
	}

	b := shimBinary(bundle, shimBinaryConfig{
		runtime:      opts.Runtime,
		address:      m.containerdAddress,
		ttrpcAddress: m.containerdTTRPCAddress,
		schedCore:    m.schedCore,
	})
	shim, err := b.Start(ctx, topts, func() {
		log.G(ctx).WithField("id", id).Info("shim disconnected")

		cleanupAfterDeadShim(context.Background(), id, ns, m.list, m.events, b)
		// Remove self from the runtime task list. Even though the cleanupAfterDeadShim()
		// would publish taskExit event, but the shim.Delete() would always failed with ttrpc
		// disconnect and there is no chance to remove this dead task from runtime task lists.
		// Thus it's better to delete it here.
		m.list.Delete(ctx, id)
	})
	if err != nil {
		return nil, errors.Wrap(err, "start failed")
	}

	return shim, nil
}

// cleanupShim attempts to properly delete and cleanup shim after error
func (m *ShimManager) cleanupShim(shim *shim) {
	dctx, cancel := timeout.WithContext(context.Background(), cleanupTimeout)
	defer cancel()

	_ = shim.delete(dctx)
	m.list.Delete(dctx, shim.ID())
}

func (m *ShimManager) Get(ctx context.Context, id string) (ShimProcess, error) {
	proc, err := m.list.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	return proc, nil
}

// Delete a runtime task
func (m *ShimManager) Delete(ctx context.Context, id string) error {
	proc, err := m.list.Get(ctx, id)
	if err != nil {
		return err
	}

	shimTask := proc.(*shimTask)
	err = shimTask.shim.delete(ctx)
	m.list.Delete(ctx, id)

	return err
}

func parsePlatforms(platformStr []string) ([]ocispec.Platform, error) {
	p := make([]ocispec.Platform, len(platformStr))
	for i, v := range platformStr {
		parsed, err := platforms.Parse(v)
		if err != nil {
			return nil, err
		}
		p[i] = parsed
	}
	return p, nil
}

// TaskManager wraps task service client on top of shim manager.
type TaskManager struct {
	shims *ShimManager
}

// NewTaskManager creates a new task manager instance.
func NewTaskManager(shims *ShimManager) *TaskManager {
	return &TaskManager{
		shims: shims,
	}
}

// ID of the task manager
func (m *TaskManager) ID() string {
	return fmt.Sprintf("%s.%s", plugin.RuntimePluginV2, "task")
}

// Create launches new shim instance and creates new task
func (m *TaskManager) Create(ctx context.Context, taskID string, opts runtime.CreateOpts) (runtime.Task, error) {
	process, err := m.shims.Start(ctx, taskID, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start shim")
	}

	shim := process.(*shimTask)
	t, err := shim.Create(ctx, opts)
	if err != nil {
		dctx, cancel := timeout.WithContext(context.Background(), cleanupTimeout)
		defer cancel()

		_, errShim := shim.delete(dctx, func(ctx context.Context, id string) {
			m.shims.list.Delete(ctx, id)
		})

		if errShim != nil {
			if errdefs.IsDeadlineExceeded(errShim) {
				dctx, cancel = timeout.WithContext(context.Background(), cleanupTimeout)
				defer cancel()
			}

			shim.Shutdown(dctx)
			shim.Close()
		}

		return nil, errors.Wrap(err, "failed to create shim task")
	}

	return t, nil
}

// Get a specific task
func (m *TaskManager) Get(ctx context.Context, id string) (runtime.Task, error) {
	return m.shims.list.Get(ctx, id)
}

// Tasks lists all tasks
func (m *TaskManager) Tasks(ctx context.Context, all bool) ([]runtime.Task, error) {
	return m.shims.list.GetAll(ctx, all)
}

// Delete deletes the task and shim instance
func (m *TaskManager) Delete(ctx context.Context, taskID string) (*runtime.Exit, error) {
	item, err := m.shims.list.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	shimTask := item.(*shimTask)
	exit, err := shimTask.delete(ctx, func(ctx context.Context, id string) {
		m.shims.list.Delete(ctx, id)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to delete task: %w", err)
	}

	return exit, nil
}

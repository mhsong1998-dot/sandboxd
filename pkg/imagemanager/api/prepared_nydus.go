// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"fmt"
	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/pkg/imagemanager/distillfs"
	"github.com/inclusionAI/sandboxd/pkg/imagemanager/nydusinput"
	"google.golang.org/protobuf/proto"
)

// PreparedNydusService mounts caller-prepared input without Registry discovery.
type PreparedNydusService interface {
	MountPreparedNydus(context.Context, *runtime.PreparedNydusSource) (*MountInfo, error)
	UmountPreparedNydus(*runtime.PreparedNydusSource) error
}

func (w *HttpWorker) MountPreparedNydus(ctx context.Context, input *runtime.PreparedNydusSource) (*MountInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := nydusinput.Validate(input); err != nil {
		return nil, err
	}
	input = proto.Clone(input).(*runtime.PreparedNydusSource)
	id, err := nydusinput.Identity(input)
	if err != nil {
		return nil, err
	}
	if w.mgr == nil {
		return nil, fmt.Errorf("distillfs manager is not initialized")
	}
	unlock, err := w.nydusOperations.Acquire(ctx, "prepared:"+id)
	if err != nil {
		return nil, err
	}
	defer unlock()
	d := w.mgr.GetDaemon(id)
	created := d == nil
	if d == nil {
		if err := w.mgr.CreateDaemon(&distillfs.DaemonCreateOpt{ID: id, Name: "prepared-" + id, SourceType: "nydus", Prepared: input}); err != nil {
			return nil, err
		}
		d = w.mgr.GetDaemon(id)
	}
	if d == nil {
		return nil, fmt.Errorf("prepared Nydus daemon is unavailable")
	}
	if err := d.MountContext(ctx); err != nil {
		w.mgr.SetDaemonReferenced(id, false)
		if created {
			return nil, errors.Join(err, w.mgr.CleanupDaemon(id))
		}
		return nil, err
	}
	w.mgr.SetDaemonReferenced(id, true)
	return &MountInfo{MountPoint: d.MountPoint(), Env: d.Env(), ImageProcess: d.ImageProcess()}, nil
}

func (w *HttpWorker) UmountPreparedNydus(input *runtime.PreparedNydusSource) error {
	id, err := nydusinput.Identity(input)
	if err != nil {
		return err
	}
	if w.mgr == nil {
		return fmt.Errorf("distillfs manager is not initialized")
	}
	d := w.mgr.GetDaemon(id)
	if d == nil {
		return nil
	}
	if err := d.Unmount(); err != nil {
		return err
	}
	w.mgr.SetDaemonReferenced(id, false)
	return nil
}

var _ PreparedNydusService = (*HttpWorker)(nil)

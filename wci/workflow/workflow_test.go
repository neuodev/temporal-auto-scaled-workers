package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/sdk"
)

// TestDeleteInstanceCancelsPendingTimer covers the race CancelTimersOnDelete targets: a delete arrives
// (explicitly via DeleteWorkerControllerInstance, or implicitly via an UpdateWorkerControllerInstance
// that removes the last scaling group) while a background timer is still pending. At
// SignalVersionWorkflowVersion the select loop can't notice the delete until that timer fires on its
// own, so the workflow waits it out; at CancelTimersOnDeleteVersion markDeleted cancels the shared timer
// context, so the pending timer resolves immediately, wakes the loop, and the workflow returns promptly.
//
// This covers the legacy timer-driven poll path, so it forces the loopDrivenPoll patch off (that patch
// arms no stats-pull timer; the loop-driven path is covered by TestLoopDrivenPollFiresFromRunLoop).
func TestDeleteInstanceCancelsPendingTimer(t *testing.T) {
	scalingConfigPayload, err := sdk.PreferProtoDataConverter.ToPayload(iface.ScalingAlgorithmConfig{})
	require.NoError(t, err)
	computeConfigPayload, err := sdk.PreferProtoDataConverter.ToPayload(map[string]any{})
	require.NoError(t, err)

	tests := []struct {
		name                 string
		workflowVersion      WorkerControllerInstanceWorkflowVersion
		wantPromptCompletion bool
		updateName           string
		updateArgs           any
	}{
		{
			name:                 "explicit delete, pre-fix version waits out the pending timer before completing",
			workflowVersion:      SignalVersionWorkflowVersion,
			wantPromptCompletion: false,
			updateName:           iface.DeleteWorkerControllerInstance,
			updateArgs:           &iface.DeleteWorkerControllerInstanceRequest{},
		},
		{
			name:                 "explicit delete, fixed version cancels the pending timer and completes promptly",
			workflowVersion:      CancelTimersOnDeleteVersion,
			wantPromptCompletion: true,
			updateName:           iface.DeleteWorkerControllerInstance,
			updateArgs:           &iface.DeleteWorkerControllerInstanceRequest{},
		},
		{
			name:                 "implicit delete (last scaling group removed), pre-fix version waits out the pending timer before completing",
			workflowVersion:      SignalVersionWorkflowVersion,
			wantPromptCompletion: false,
			updateName:           iface.UpdateWorkerControllerInstance,
			updateArgs:           &iface.UpdateWorkerControllerInstanceRequest{RemoveScalingGroups: []string{"workflow"}},
		},
		{
			name:                 "implicit delete (last scaling group removed), fixed version cancels the pending timer and completes promptly",
			workflowVersion:      CancelTimersOnDeleteVersion,
			wantPromptCompletion: true,
			updateName:           iface.UpdateWorkerControllerInstance,
			updateArgs:           &iface.UpdateWorkerControllerInstanceRequest{RemoveScalingGroups: []string{"workflow"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			activities := NewActivities(nil, nil, nil)
			args := &iface.WorkerControllerInstanceWorkflowArgs{
				NamespaceName:  "test-namespace",
				DeploymentName: "test-deployment",
				BuildId:        "test-build",
				State: &iface.WorkerControllerInstanceLocalState{
					Spec: &iface.WorkerControllerInstanceSpec{
						ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{
							"workflow": newTestScalingGroupSpec(enumspb.TASK_QUEUE_TYPE_WORKFLOW, scalingConfigPayload, computeConfigPayload),
						},
					},
				},
			}

			// Mirrors how wci/workercomponent/component.go wires Workflow, but pins
			// the version directly instead of reading it from dynamic config.
			testWorkflow := func(ctx sdkworkflow.Context, args *iface.WorkerControllerInstanceWorkflowArgs) error {
				return Workflow(ctx,
					func() WorkerControllerInstanceWorkflowVersion { return tc.workflowVersion },
					func() int { return 100 },
					func() time.Duration { return periodicValidationInterval },
					args, activities)
			}

			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.RegisterWorkflow(testWorkflow)

			// Force the loopDrivenPoll patch off so the stats-pull timer is armed (this test covers the
			// legacy timer-driven path).
			env.OnGetVersion(loopDrivenPollPatch, sdkworkflow.DefaultVersion, 1).Return(sdkworkflow.DefaultVersion)

			env.OnActivity(activities.PullStats, mock.Anything, mock.Anything).
				Return(&PullStatsActivityResponse{NextPollSeconds: uint32(maxPollInterval.Seconds())}, nil)

			env.RegisterDelayedCallback(func() {
				env.UpdateWorkflowNoRejection(tc.updateName, "update-1", t, tc.updateArgs)
			}, time.Millisecond)

			startTime := env.Now()
			env.ExecuteWorkflow(testWorkflow, args)
			elapsed := env.Now().Sub(startTime)

			require.True(t, env.IsWorkflowCompleted())
			require.NoError(t, env.GetWorkflowError())
			if tc.wantPromptCompletion {
				require.Less(t, elapsed, time.Second, "expected the workflow to complete promptly after delete, without waiting out the pending timer")
			} else {
				require.GreaterOrEqual(t, elapsed, maxPollInterval, "expected the workflow to wait out the pending stats-pull timer before completing")
			}
		})
	}
}

// TestLoopDrivenPollFiresFromRunLoop verifies the loop-driven poll: with the patch on there is no
// stats-pull timer, so PullStats can only fire from pollIfDue in the run loop. A NextPollTime in the
// past makes the first loop lap immediately due, and the poll runs without any selector timer.
func TestLoopDrivenPollFiresFromRunLoop(t *testing.T) {
	scalingConfigPayload, err := sdk.PreferProtoDataConverter.ToPayload(iface.ScalingAlgorithmConfig{})
	require.NoError(t, err)
	computeConfigPayload, err := sdk.PreferProtoDataConverter.ToPayload(map[string]any{})
	require.NoError(t, err)

	activities := NewActivities(nil, nil, nil)
	args := &iface.WorkerControllerInstanceWorkflowArgs{
		NamespaceName:  "test-namespace",
		DeploymentName: "test-deployment",
		BuildId:        "test-build",
		State: &iface.WorkerControllerInstanceLocalState{
			// A deadline in the distant past: the first loop lap is immediately due to poll.
			NextPollTime: timestamppb.New(time.Unix(1, 0)),
			Spec: &iface.WorkerControllerInstanceSpec{
				ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{
					"workflow": newTestScalingGroupSpec(enumspb.TASK_QUEUE_TYPE_WORKFLOW, scalingConfigPayload, computeConfigPayload),
				},
			},
		},
	}

	// loop-driven is on by default in the test env (the loopDrivenPoll patch returns its max version).
	testWorkflow := func(ctx sdkworkflow.Context, args *iface.WorkerControllerInstanceWorkflowArgs) error {
		return Workflow(ctx,
			func() WorkerControllerInstanceWorkflowVersion { return CancelTimersOnDeleteVersion },
			func() int { return 100 },
			func() time.Duration { return periodicValidationInterval },
			args, activities)
	}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(testWorkflow)

	pullStatsCalled := false
	env.OnActivity(activities.PullStats, mock.Anything, mock.Anything).
		Return(&PullStatsActivityResponse{NextPollSeconds: uint32(maxPollInterval.Seconds())}, nil).
		Run(func(mock.Arguments) { pullStatsCalled = true })

	// Delete shortly after start so the workflow completes (CancelTimersOnDeleteVersion cancels the
	// pending timers and wakes the loop); the loop-driven poll has already fired at start.
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection(iface.DeleteWorkerControllerInstance, "del-1", t, &iface.DeleteWorkerControllerInstanceRequest{})
	}, time.Millisecond)

	env.ExecuteWorkflow(testWorkflow, args)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.True(t, pullStatsCalled, "loop-driven poll must fire PullStats from the run loop, with no stats-pull timer")
}

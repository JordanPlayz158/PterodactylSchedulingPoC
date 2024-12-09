<?php

namespace Pterodactyl\Jobs\Schedule;

use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;
use Pterodactyl\Jobs\Job;
use Carbon\CarbonImmutable;
use Pterodactyl\Models\Task;
use Illuminate\Queue\SerializesModels;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\DispatchesJobs;
use Pterodactyl\Repositories\Wings\DaemonServerRepository;
use Pterodactyl\Services\Backups\InitiateBackupService;
use Pterodactyl\Repositories\Wings\DaemonPowerRepository;
use Pterodactyl\Repositories\Wings\DaemonCommandRepository;
use Pterodactyl\Exceptions\Http\Connection\DaemonConnectionException;

class RunTaskJob extends Job implements ShouldQueue
{
    use DispatchesJobs;
    use InteractsWithQueue;
    use SerializesModels;

    // How often to poll for changes (in microseconds)
    // Default is 1,000,000 or 1 second
    private int $pollingFrequency;

    /**
     * RunTaskJob constructor.
     */
    public function __construct(public Task $task, public bool $manualRun = false)
    {
        $this->queue = 'standard';
        $this->timeout = env('TASK_TIMEOUT', 60);
        $this->pollingFrequency = env('TASK_ACTION_STATUS_POLLING_FREQUENCY', 1000000);
    }

    /**
     * Run the job and send actions to the daemon running the server.
     *
     * @throws \Throwable
     */
    public function handle(
        DaemonServerRepository $serverRepository,
        DaemonCommandRepository $commandRepository,
        InitiateBackupService $backupService,
        DaemonPowerRepository $powerRepository
    ) {
        $task = $this->task;

        // Do not process a task that is not set to active, unless it's been manually triggered.
        if (!$task->schedule->is_active && !$this->manualRun) {
            $this->markTaskNotQueued();
            $this->markScheduleComplete();

            return;
        }

        $server = $task->server;
        // If we made it to this point and the server status is not null it means the
        // server was likely suspended or marked as reinstalling after the schedule
        // was queued up. Just end the task right now — this should be a very rare
        // condition.
        if (!is_null($server->status)) {
            $this->failed();

            return;
        }

        $uuid = $server->uuid;
        $action = $task->action;
        $payload = $task->payload;

        Log::channel('job')->info("[RunTaskJob] Performing action: $action with payload: $payload", ['server' => $uuid]);

        // Perform the provided task against the daemon.
        try {
            switch ($action) {
                case Task::ACTION_POWER:
                    $powerRepository->setServer($server)->send($payload);

                    $state = match ($payload) {
                        'stop', 'terminate' => 'offline',
                        'start', 'restart' => 'running',
                        default => throw new \Exception('Invalid power action, task exiting')
                    };

                    $serverRepository->setServer($server);

                    $actualState = Arr::get($serverRepository->getDetails(), 'state', 'stopped');
                    while ($actualState != $state) {
                        Log::channel('job')->info("[RunTaskJob] Payload: $payload, state: $state, status:", ['server' => $uuid, 'state' => $actualState]);
                        usleep($this->pollingFrequency);
                        $actualState = Arr::get($serverRepository->getDetails(), 'state', 'stopped');
                    }

                    break;
                case Task::ACTION_COMMAND:
                    $commandRepository->setServer($server)->send($payload);
                    // Not sure if I can verify this was indeed sent before next task withOUT
                    //  adding a websocket client dependency and opening a connection
                    //  but given the ease of this task, it mayyy be fine unless you
                    //  chain multiple command payloads without any delay THEN... it may be possible
                    //  and/or an issue depending on how wings handles command payloads
                    break;
                case Task::ACTION_BACKUP:
                    $backup = $backupService->setIgnoredFiles(explode(PHP_EOL, $payload))->handle($server, null, true);

                    // 2 backups in 600 seconds will cause the task to fail

                    while ($backup->refresh()->completed_at == null) {
                        usleep($this->pollingFrequency);
                    }
                    break;
                default:
                    throw new \InvalidArgumentException('Invalid task action provided: ' . $action);
            }
        } catch (\Exception $exception) {
            // If this isn't a DaemonConnectionException on a task that allows for failures
            // throw the exception back up the chain so that the task is stopped.
            if (!($task->continue_on_failure && $exception instanceof DaemonConnectionException)) {
                throw $exception;
            }
        }

        $this->markTaskNotQueued();
        $this->queueNextTask();
    }

    /**
     * Handle a failure while sending the action to the daemon or otherwise processing the job.
     */
    public function failed(\Exception $exception = null)
    {
        $this->markTaskNotQueued();
        $this->markScheduleComplete();
    }

    /**
     * Get the next task in the schedule and queue it for running after the defined period of wait time.
     */
    private function queueNextTask()
    {
        $task = $this->task;

        /** @var \Pterodactyl\Models\Task|null $nextTask */
        $nextTask = Task::query()->where('schedule_id', $task->schedule_id)
            ->orderBy('sequence_id', 'asc')
            ->where('sequence_id', '>', $task->sequence_id)
            ->first();

        if (is_null($nextTask)) {
            $this->markScheduleComplete();

            return;
        }

        Log::channel('job')->info(
            "[RunTaskJob] Next Task with schedule_id: $task->schedule_id and sequence_id > $task->sequence_id",
            ['task' => $nextTask]);

        $nextTask->update(['is_queued' => true]);

        $this->dispatch((new self($nextTask, $this->manualRun))->delay($nextTask->time_offset));
    }

    /**
     * Marks the parent schedule as being complete.
     */
    private function markScheduleComplete()
    {
        $this->task->schedule()->update([
            'is_processing' => false,
            'last_run_at' => CarbonImmutable::now()->toDateTimeString(),
        ]);
    }

    /**
     * Mark a specific task as no longer being queued.
     */
    private function markTaskNotQueued()
    {
        $this->task->update(['is_queued' => false]);
    }
}

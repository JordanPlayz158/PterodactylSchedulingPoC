<?php

namespace Pterodactyl\Jobs\Schedule;

use Illuminate\Cache\RedisStore;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Cache;
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

    /**
     * RunTaskJob constructor.
     */
    public function __construct(public Task $task, public bool $manualRun = false)
    {
        $this->queue = 'standard';
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

        $powerLock = null;

        // Perform the provided task against the daemon.
        try {
            switch ($action) {
                case Task::ACTION_POWER:
                    // Check if lock exists and if process is still running
                    //  to account for weird cases where lock is not released
                    //  by previous process
                    $cacheClient = Cache::getStore();

                    // Based off of this https://stackoverflow.com/a/76867663/12005894
                    //  not sure why redis store in specific has a lock connection
                    //  couldn't find anything online about it but hopefully this
                    //  should mean it will work with and without redis
                    //
                    //  I only test with redis as cache though
                    if ($cacheClient instanceof RedisStore) $cacheClient = $cacheClient->lockConnection()->client();

                    $cacheKey = Cache::getPrefix() . $uuid;
                    $previousOwnerEntry = $cacheClient->get($cacheKey);

                    if ($previousOwnerEntry) {
                        $previousOwner = explode('-', $previousOwnerEntry);
                        $previousPid = $previousOwner[0];
                        $previousPidModifiedTime = $previousOwner[1];

                        // Can't think of a better name
                        $currentPreviousPidModifiedTime = filemtime("/proc/$previousPid");

                        // Can't get creation time on unix so not quite sure if this is good enough
                        //  all depends on when linux modifies the pid file of which I don't know the extent of
                        // Also added the modified time because PIDs are re-used so false positives could occur without it
                        if (!$currentPreviousPidModifiedTime
                            || $currentPreviousPidModifiedTime != $previousPidModifiedTime) {
                            $cacheClient->forget($cacheKey);
                        }
                    }

                    $pid = getmypid();
                    $pidModifiedTime = filemtime("/proc/$pid");
                    $powerLock = Cache::lock($uuid, 0, "$pid-$pidModifiedTime");
                    // Not sure what time is reasonable to wait for the lock to acquire
                    $powerLock->block(5);

                    Log::channel('job')->info('[RunTaskJob] Lock was locked', ['server' => $uuid, 'lock' => $powerLock]);

                    $powerRepository->setServer($server)->send($payload, true);

                    $state = "notSet";
                    switch ($payload) {
                        case "stop":
                        case "terminate":
                            $state = "offline";
                            break;
                        case "start":
                        case "restart":
                            $state = "running";
                    }

                    $seconds = 0;
                    $actualState = Arr::get($serverRepository->setServer($server)->getDetails(), 'state', 'stopped');
                    while ($actualState != $state
                        // Arbitrary 10 minute timeout in case it gets stuck for whatever reason
                        //  do need to test what happens with cron and laravel if the process drags
                        //  on too long, does it run 2 schedule:process commands at once leading to
                        //  mayhem or... does cron or laravel only allow 1 at a time, I'd hope one
                        //  of the same command at a time
                        && $seconds < 600) {
                        Log::channel('job')->info("[RunTaskJob] Payload: $payload, state: $state, status:", ['server' => $uuid, 'state' => $actualState]);
                        sleep(1);
                        $seconds++;
                        $actualState = Arr::get($serverRepository->setServer($server)->getDetails(), 'state', 'stopped');
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

                    $seconds = 0;
                    while ($backup->refresh()->completed_at == null && $seconds < 600) {
                        sleep(1);
                        $seconds++;
                    }
                    break;
                default:
                    throw new \InvalidArgumentException('Invalid task action provided: ' . $action);
            }
        } catch (\Exception $exception) {
            if ($exception instanceof LockTimeoutException) {
                Log::channel('job')->info('[RunTaskJob] Lock could not be acquired while blocking for 5 seconds, job exiting', ['server' => $uuid, 'lock' => $powerLock]);
            }

            // If this isn't a DaemonConnectionException on a task that allows for failures
            // throw the exception back up the chain so that the task is stopped.
            if (!($task->continue_on_failure && $exception instanceof DaemonConnectionException)) {
                // Think I need the lock false in both catch and finally as catch runs first
                //  and in some instances the catch will throw an exception that is not caught
                //  which might mean the finally wouldn't be executed in this case
                throw $exception;
            }
        } finally {
            if ($powerLock != null) {
                Log::channel('job')->info('[RunTaskJob] Lock was released', ['server' => $uuid, 'lock' => $powerLock]);
                $powerLock->release();
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

        Log::channel('job')->info(
            "[RunTaskJob] Next Task with schedule_id: $task->schedule_id and sequence_id > $task->sequence_id",
            ['task' => $nextTask]);

        if (is_null($nextTask)) {
            $this->markScheduleComplete();

            return;
        }

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

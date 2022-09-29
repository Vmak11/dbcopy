<?php

namespace Vmak11\DbCopy;

use Exception;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

class Processor
{
    /**
     * @var Process[]
     */
    private $queued;

    /**
     * @var Process[]
     */
    private $running;

    /**
     * @var int
     */
    private $threads;

    /**
     * @var int
     */
    private $timeLimit;

    /**
     * Processor constructor.
     * @param int $threads
     * @param int $timeLimit
     */
    public function __construct(int $threads = 1, int $timeLimit = 0)
    {
        $this->threads = $threads;
        $this->queued = [];
        $this->running = [];
        $this->timeLimit = $timeLimit;
    }

    /**
     * Add a command to be executed
     *
     * @param string $command
     * @return $this
     */
    public function addCommand(string $command): Processor
    {
        $this->queued[] = $this->createProcessFromCommand($command);

        return $this;
    }

    /**
     * @param string $command
     * @return Process
     */
    protected function createProcessFromCommand(string $command): Process
    {
        return Process::fromShellCommandline($command);
    }

    /**
     * @param $threads
     * @return $this
     */
    public function setThreads($threads): Processor
    {
        $this->threads = $threads;

        return $this;
    }

    /**
     * @return array|Process[]
     */
    public function getQueued(): array
    {
        return $this->queued;
    }

    /**
     * Run all the queued commands
     *
     * @return array
     * @throws ProcessFailedException
     * @throws Exception
     */
    public function run(): array
    {
        if (empty($this->queued)) {
            throw new Exception('Queue is empty, nothing to run');
        }

        // Set to defined time limit
        set_time_limit($this->timeLimit);

        $completed = [];
        // While we still have queued or running commands
        while (!empty($this->queued) || !empty($this->running)) {
            // If we have more commands to start and are not yet at our max thread limit
            if (!empty($this->queued) && count($this->running) < $this->threads) {
                // Get the first process from queued array, start it and add it to running array
                $process = array_shift($this->queued);
                $process->start();
                $this->running[] = $process;
            }

            // Check to see if any of our running processes have completed and update arrays accordingly
            foreach ($this->running as $index => $running_process) {
                // If process is still running or waiting to run
                if ($running_process->isRunning()) {
                    continue;
                }

                if (!$running_process->isSuccessful()) {
                    throw new ProcessFailedException($running_process);
                }

                $completed[] = $running_process;
                unset($this->running[$index]);
            }
        }

        return $completed;
    }
}
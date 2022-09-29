<?php

namespace Vmak11\Tests;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;
use Vmak11\DbCopy\Processor;

class ProcessorTest extends TestCase
{
    public function testAddCommand()
    {
        $processor = new Processor(8);
        $command = 'test';
        $processor->addCommand($command);
        $process = Process::fromShellCommandline($command);
        $queued = [
            $process,
        ];
        $this->assertEquals($queued, $processor->getQueued());
    }

    public function testRun()
    {
        $processor = $this->getMockBuilder(Processor::class)
            ->setMethods(['createProcessFromCommand'])
            ->setConstructorArgs([1])
            ->getMock();

        $process = $this->getMockBuilder(Process::class)
            ->setMethods([
                'start',
                'isRunning',
                'isSuccessful',
            ])->disableOriginalConstructor()
            ->getMock();

        $process->expects($this->once())->method('start');
        $process->expects($this->once())->method('isRunning')->willReturn(false);
        $process->expects($this->once())->method('isSuccessful')->willReturn(true);

        $processor->expects($this->once())->method('createProcessFromCommand')
            ->with('test')->willReturn($process);

        $processor->addCommand('test');

        $this->assertEquals([$process], $processor->run());
    }

    public function testRunEmptyThrowsException()
    {
        $processor = $this->getMockBuilder(Processor::class)
            ->setMethods(['createProcessFromCommand'])
            ->setConstructorArgs([1])
            ->getMock();

        $this->expectExceptionMessage('Queue is empty, nothing to run');
        $processor->run();
    }

    public function testRunWithException()
    {
        $processor = $this->getMockBuilder(Processor::class)
            ->setMethods(['createProcessFromCommand'])
            ->setConstructorArgs([1])
            ->getMock();

        $process = $this->getMockBuilder(Process::class)
            ->setMethods([
                'start',
                'isRunning',
                'isSuccessful',
                'getOutput',
                'getErrorOutput',
                'stop',
            ])->disableOriginalConstructor()
            ->getMock();

        $process->expects($this->once())->method('start');
        $process->expects($this->once())->method('isRunning')->willReturn(false);
        $process->expects($this->exactly(2))->method('isSuccessful')->willReturn(false);

        $processor->expects($this->once())->method('createProcessFromCommand')
            ->with('test')->willReturn($process);

        $processor->addCommand('test');

        $this->expectException(ProcessFailedException::class);
        $processor->run();
    }

    public function testRunWillUtilizeMultipleThreads()
    {
        $processor = new Processor(2);

        $command1 = "php -r 'sleep(1);'";
        $command2 = "php -r 'sleep(0);'";

        $processor->addCommand($command1);
        $processor->addCommand($command2);

        /** @var Process[] $completed */
        $completed = $processor->run();
        // We are asserting that the faster running command ran asynchronously and completed first
        $this->assertEquals($command2, $completed[0]->getCommandLine());
        $this->assertEquals($command1, $completed[1]->getCommandLine());
    }
}
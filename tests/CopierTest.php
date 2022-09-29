<?php

namespace Vmak11\Tests;

use PHPUnit\Framework\TestCase;
use Vmak11\DbCopy\Copier;
use Vmak11\DbCopy\Helpers\MySql;
use Vmak11\DbCopy\Processor;

class CopierTest extends TestCase
{
    public function testExcludeTables()
    {
        $copier = $this->getMockBuilder(Copier::class)
            ->setMethodsExcept([
                'includeTables',
                'excludeTables',
            ])
            ->disableOriginalConstructor()
            ->getMock();

        $copier->includeTables(['test1']);
        $this->expectExceptionMessage('Can not exclude tables when include tables is not empty.');
        $copier->excludeTables(['test2']);
    }

    public function testIncludeTables()
    {
        $copier = $this->getMockBuilder(Copier::class)
            ->setMethodsExcept([
                'includeTables',
                'excludeTables',
            ])
            ->disableOriginalConstructor()
            ->getMock();

        $copier->excludeTables(['test1']);
        $this->expectExceptionMessage('Can not include tables when exclude tables is not empty.');
        $copier->includeTables(['test2']);
    }

    public function testExcludeDataFor()
    {
        $copier = $this->getMockBuilder(Copier::class)
            ->setMethodsExcept([
                'includeDataFor',
                'excludeDataFor',
            ])
            ->disableOriginalConstructor()
            ->getMock();

        $copier->includeDataFor(['test1']);
        $this->expectExceptionMessage('Can not exclude table data when include table data is not empty.');
        $copier->excludeDataFor(['test2']);
    }

    public function testIncludeDataFor()
    {
        $copier = $this->getMockBuilder(Copier::class)
            ->setMethodsExcept([
                'includeDataFor',
                'excludeDataFor',
            ])
            ->disableOriginalConstructor()
            ->getMock();

        $copier->excludeDataFor(['test1']);
        $this->expectExceptionMessage('Can not include table data when exclude table data is not empty.');
        $copier->includeDataFor(['test2']);
    }

    public function testGetAllTablesWillThrowExceptionForEmptyTables()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = new Copier($processor, $dbHelper);
        $dbHelper->expects($this->once())->method('getTables')
            ->willReturn([]);

        $this->expectExceptionMessage('Tables list is empty');
        $copier->getAllTables();
    }

    public function testGetAllTablesWillThrowExceptionForMissingIncludedTable()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = new Copier($processor, $dbHelper);
        $dbHelper->expects($this->once())->method('getTables')
            ->willReturn(['table1', 'table2']);

        $copier->includeTables(['table1', 'table2', 'table3']);
        $this->expectExceptionMessage("Included table `table3` does not exist in the read database");
        $copier->getAllTables();
    }

    public function testGetAllTablesWillThrowExceptionForMissingExcludedTable()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = new Copier($processor, $dbHelper);
        $dbHelper->expects($this->once())->method('getTables')
            ->willReturn(['table1', 'table2']);

        $copier->excludeTables(['table1', 'table2', 'table3']);
        $this->expectExceptionMessage("Excluded table `table3` does not exist in the read database");
        $copier->getAllTables();
    }

    public function testGetAllTablesWillReturnAllTablesWhenNotEmpty()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = new Copier($processor, $dbHelper);
        $allTables = ['table1', 'table2'];
        $copier->setAllTables($allTables);
        $this->assertEquals($allTables, $copier->getAllTables());
    }

    public function testGetAllTablesWillThrowExceptionForIncludedTableThatDoesNotExist()
    {
        $dbHelper = $this->createMock(MySql::class);
        $dbHelper->expects($this->once())->method('getTables')->willReturn(['table1', 'table2', 'table3']);
        $processor = $this->createMock(Processor::class);

        $copier = new Copier($processor, $dbHelper);

        $copier->includeTables(['table1', 'table2', 'table3', 'table4']);
        $this->expectExceptionMessage('Included table `table4` does not exist in the read database');
        $copier->getAllTables();
    }

    public function testGetAllTablesWillThrowExceptionForExcludedTableThatDoesNotExist()
    {
        $dbHelper = $this->createMock(MySql::class);
        $dbHelper->expects($this->once())->method('getTables')->willReturn(['table1', 'table2', 'table3']);
        $processor = $this->createMock(Processor::class);

        $copier = new Copier($processor, $dbHelper);

        $copier->excludeTables(['table3', 'table4']);
        $this->expectExceptionMessage('Excluded table `table4` does not exist in the read database');
        $copier->getAllTables();
    }


    public function testGetAllTablesWithDataWillReturnAllTablesWhenNotEmpty()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = new Copier($processor, $dbHelper);
        $allTables = ['table1', 'table2'];
        $copier->setAllTablesWithData($allTables);
        $this->assertEquals($allTables, $copier->getAllTablesWithData());
    }

    public function testGetAllTablesWithDataWillThrowExceptionForIncludedTableThatDoesNotExist()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = $this->getMockBuilder(Copier::class)
            ->setConstructorArgs([$processor, $dbHelper])
            ->setMethods(['getAllTables'])
            ->getMock();

        $copier->expects($this->once())->method('getAllTables')->willReturn(['table1', 'table2', 'table3']);

        $copier->includeDataFor(['table1', 'table2', 'table3', 'table4']);
        $this->expectExceptionMessage('Table `table4` was defined to include data but does not exist in table array');
        $copier->getAllTablesWithData();
    }

    public function testGetAllTablesWithDataWillThrowExceptionForExcludedTableThatDoesNotExist()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = $this->getMockBuilder(Copier::class)
            ->setConstructorArgs([$processor, $dbHelper])
            ->setMethods(['getAllTables'])
            ->getMock();

        $copier->expects($this->once())->method('getAllTables')->willReturn(['table1', 'table2', 'table3']);

        $copier->excludeDataFor(['table3', 'table4']);
        $this->expectExceptionMessage('Table `table4` was defined to exclude data but does not exist in table array');
        $copier->getAllTablesWithData();
    }

    public function testCopySchema()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = $this->getMockBuilder(Copier::class)
            ->setConstructorArgs([$processor, $dbHelper])
            ->setMethods(['getAllTables'])
            ->getMock();

        $copier->expects($this->once())->method('getAllTables')->willReturn(['table1', 'table2', 'table3']);
        $dbHelper->expects($this->once())->method('createDatabaseCommand')->willReturn('create database');
        $dbHelper->expects($this->exactly(3))->method('createCopySchemaCommand')
            ->willReturnOnConsecutiveCalls('create table1', 'create table2', 'create table3');
        $processor->expects($this->exactly(4))->method('addCommand')
            ->withConsecutive(['create database'], ['create table1'], ['create table2'], ['create table3'])
            ->willReturn($processor);
        $processor->expects($this->exactly(4))->method('run');

        $copier->copySchema();
    }

    public function testCopyTriggers()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = $this->getMockBuilder(Copier::class)
            ->setConstructorArgs([$processor, $dbHelper])
            ->setMethods(['getAllTables'])
            ->getMock();

        $copier->expects($this->once())->method('getAllTables')->willReturn(['table1', 'table2', 'table3']);
        $dbHelper->expects($this->exactly(3))->method('createCopyTriggersCommand')
            ->willReturnOnConsecutiveCalls('copy triggers for table1', 'copy triggers for table2',
                'copy triggers for table3');
        $processor->expects($this->exactly(3))->method('addCommand')
            ->withConsecutive(['copy triggers for table1'], ['copy triggers for table2'], ['copy triggers for table3'])
            ->willReturn($processor);
        $processor->expects($this->once())->method('run');

        $copier->copyTriggers();
    }

    public function testCopyData()
    {
        $dbHelper = $this->createMock(MySql::class);
        $processor = $this->createMock(Processor::class);

        $copier = $this->getMockBuilder(Copier::class)
            ->setConstructorArgs([$processor, $dbHelper])
            ->setMethods(['getAllTablesWithData'])
            ->getMock();

        $copier->expects($this->once())->method('getAllTablesWithData')->willReturn(['table1', 'table2', 'table3']);

        $copier->setRowLimit(50);

        $dbHelper->expects($this->exactly(3))->method('getRowCount')
            ->withConsecutive(['table1'], ['table2'], ['table3'])
            ->willReturnOnConsecutiveCalls(10, 50, 101);

        $dbHelper->expects($this->exactly(2))->method('createCopyDataCommand')
            ->withConsecutive(['table1'], ['table2'])
            ->willReturnOnConsecutiveCalls('copy table1', 'copy table2');

        $dbHelper->expects($this->exactly(3))->method('createCopyChunkedDataCommand')
            ->withConsecutive(['table3', 50, 0], ['table3', 50, 50], ['table3', 50, 100])
            ->willReturnOnConsecutiveCalls('copy table3 chunked 0', 'copy table3 chunked 50',
                'copy table3 chunked 100');

        $processor->expects($this->exactly(5))->method('addCommand')
            ->withConsecutive(['copy table1'], ['copy table2'], ['copy table3 chunked 0'], ['copy table3 chunked 50'],
                ['copy table3 chunked 100'])
            ->willReturn($processor);

        $processor->expects($this->once())->method('run');

        $copier->copyData();
    }

    public function testCopyAll()
    {
        $copier = $this->getMockBuilder(Copier::class)
            ->disableOriginalConstructor()
            ->setMethods(['copySchema', 'copyData', 'copyTriggers'])
            ->getMock();

        $copier->expects($this->exactly(2))->method('copySchema');
        $copier->expects($this->exactly(2))->method('copyData');
        $copier->expects($this->exactly(1))->method('copyTriggers');

        $copier->copyAll();
        $copier->setCopyTriggers(false);
        $copier->copyAll();
    }
}
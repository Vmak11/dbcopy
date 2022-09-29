<?php

namespace Vmak11\DbCopy;

use Exception;

/**
 * Class Copier
 */
class Copier
{
    /**
     * @var Processor
     */
    private $processor;

    /**
     * @var int
     */
    private $rowLimit;

    /**
     * @var DbHelper
     */
    private $dbHelper;

    /**
     * @var array
     */
    private $allTables;

    /**
     * @var array
     */
    private $allTablesWithData;

    /**
     * @var array
     */
    private $includeTables;

    /**
     * @var array
     */
    private $excludeTables;

    /**
     * @var array
     */
    private $includeDataFor;

    /**
     * @var array
     */
    private $excludeDataFor;

    /**
     * @var bool
     */
    private $copyTriggers = true;

    /**
     * Copier constructor.
     *
     * @param Processor $processor
     * @param DbHelper $dbHelper
     */
    public function __construct(Processor $processor, DbHelper $dbHelper)
    {
        $this->processor = $processor;
        $this->dbHelper = $dbHelper;
    }

    /**
     * @param int $rowLimit
     * @return $this
     */
    public function setRowLimit(int $rowLimit): Copier
    {
        $this->rowLimit = $rowLimit;

        return $this;
    }

    /**
     * @param bool $copyTriggers
     * @return $this
     */
    public function setCopyTriggers(bool $copyTriggers): Copier
    {
        $this->copyTriggers = $copyTriggers;

        return $this;
    }

    /**
     * Define tables to exclude
     *
     * @param array $excludeTables
     * @return $this
     * @throws Exception
     */
    public function excludeTables(array $excludeTables): Copier
    {
        if (!empty($this->includeTables)) {
            throw new Exception('Can not exclude tables when include tables is not empty.');
        }

        $this->excludeTables = $excludeTables;

        return $this;
    }

    /**
     * Define tables to be included, all others will be excluded
     *
     * @param array $includeTables
     * @return $this
     * @throws Exception
     */
    public function includeTables(array $includeTables): Copier
    {
        if (!empty($this->excludeTables)) {
            throw new Exception('Can not include tables when exclude tables is not empty.');
        }

        $this->includeTables = $includeTables;

        return $this;
    }

    /**
     * Define tables to exclude data for
     *
     * @param array $excludeDataFor
     * @return $this
     * @throws Exception
     */
    public function excludeDataFor(array $excludeDataFor): Copier
    {
        if (!empty($this->includeDataFor)) {
            throw new Exception('Can not exclude table data when include table data is not empty.');
        }

        $this->excludeDataFor = $excludeDataFor;

        return $this;
    }

    /**
     * Define tables to include data for
     *
     * @param array $includeDataFor
     * @return $this
     * @throws Exception
     */
    public function includeDataFor(array $includeDataFor): Copier
    {
        if (!empty($this->excludeDataFor)) {
            throw new Exception('Can not include table data when exclude table data is not empty.');
        }

        $this->includeDataFor = $includeDataFor;

        return $this;
    }

    /**
     * Copy database from read database to write database
     *
     * @throws Exception
     */
    public function copyAll()
    {
        // Copy schema only first
        $this->copySchema();

        // Copy data
        $this->copyData();

        // Copy triggers
        if ($this->copyTriggers) {
            $this->copyTriggers();
        }
    }

    /**
     * Copy database schema for all defined tables
     *
     * @throws Exception
     */
    public function copySchema()
    {
        // Create write database if it doesn't exist
        $this->processor->addCommand($this->dbHelper->createDatabaseCommand())->run();

        // Run copy schema commands
        foreach ($this->getAllTables() as $table) {
            $this->processor->addCommand($this->dbHelper->createCopySchemaCommand($table))->run();
        }
    }

    /**
     * Get an array of tables we want to copy
     *
     * @return array
     * @throws Exception
     */
    public function getAllTables(): array
    {
        if (!empty($this->allTables)) {
            return $this->allTables;
        }

        $allTables = $this->dbHelper->getTables();

        if (!empty($this->includeTables)) {
            foreach ($this->includeTables as $includeTable) {
                if (!in_array($includeTable, $allTables)) {
                    throw new Exception("Included table `$includeTable` does not exist in the read database");
                }
            }

            return $this->allTables = $this->includeTables;
        }

        if (!empty($this->excludeTables)) {
            foreach ($this->excludeTables as $excludeTable) {
                if (!in_array($excludeTable, $allTables)) {
                    throw new Exception("Excluded table `$excludeTable` does not exist in the read database");
                }
            }
            $allTables = array_diff($allTables, $this->excludeTables);
        }

        if (empty($allTables)) {
            throw new Exception('Tables list is empty');
        }

        return $this->allTables = $allTables;
    }

    /**
     * @param array $allTables
     * @return Copier
     */
    public function setAllTables(array $allTables): Copier
    {
        $this->allTables = $allTables;

        return $this;
    }

    /**
     * Copy database records for all defined tables with data
     *
     * @throws Exception
     */
    public function copyData()
    {
        $tablesWithData = $this->getAllTablesWithData();

        foreach ($tablesWithData as $table) {
            // Determine how many records the table contains
            $rowCount = $this->dbHelper->getRowCount($table);

            // If row count is higher than our limit, split into multiple chunks to handle in multiple threads
            if (!is_null($this->rowLimit) && $rowCount > $this->rowLimit) {
                $offset = 0;
                while ($offset < $rowCount) {
                    $this->processor->addCommand($this->dbHelper->createCopyChunkedDataCommand($table, $this->rowLimit,
                        $offset));
                    $offset += $this->rowLimit;
                }
                continue;
            }

            $this->processor->addCommand($this->dbHelper->createCopyDataCommand($table));
        }
        $this->processor->run();
    }

    /**
     * Get an array of tables we want to copy with data
     *
     * @return array
     * @throws Exception
     */
    public function getAllTablesWithData(): array
    {
        if (!empty($this->allTablesWithData)) {
            return $this->allTablesWithData;
        }

        $tables = $this->getAllTables();
        if (!empty($this->includeDataFor)) {
            foreach ($this->includeDataFor as $includeDataFor) {
                if (!in_array($includeDataFor, $tables)) {
                    throw new Exception("Table `$includeDataFor` was defined to include data but does not exist in table array");
                }
            }

            return $this->allTablesWithData = $this->includeDataFor;
        }

        if (!empty($this->excludeDataFor)) {
            foreach ($this->excludeDataFor as $excludeDataFor) {
                if (!in_array($excludeDataFor, $tables)) {
                    throw new Exception("Table `$excludeDataFor` was defined to exclude data but does not exist in table array");
                }
            }
            $tables = array_diff($tables, $this->excludeDataFor);
        }

        return $this->allTablesWithData = $tables;
    }

    /**
     * @param array $allTablesWithData
     * @return Copier
     */
    public function setAllTablesWithData(array $allTablesWithData): Copier
    {
        $this->allTablesWithData = $allTablesWithData;

        return $this;
    }

    /**
     * Copy database triggers for all defined tables
     *
     * @throws Exception
     */
    public function copyTriggers()
    {
        foreach ($this->getAllTables() as $table) {
            $this->processor->addCommand($this->dbHelper->createCopyTriggersCommand($table));
        }
        $this->processor->run();
    }
}
<?php

namespace Vmak11\DbCopy;

abstract class DbHelper
{
    /**
     * @var Config
     */
    protected $readConfig;

    /**
     * @var Config
     */
    protected $writeConfig;

    /**
     * @param Config $readConfig
     * @param Config $writeConfig
     */
    public function __construct(Config $readConfig, Config $writeConfig)
    {
        $this->readConfig = $readConfig;
        $this->writeConfig = $writeConfig;
    }

    /**
     * Get an array of tables that exist within the read database
     *
     * @return array
     */
    abstract public function getTables(): array;

    /**
     * Get the amount of rows in a table from the read database
     *
     * @param string $table
     * @return int
     */
    abstract public function getRowCount(string $table): int;

    /**
     * Generate a command for creating a new database
     *
     * @return string
     */
    abstract public function createDatabaseCommand(): string;

    /**
     * Generate a command for copying a table (schema only)
     *
     * @param string $table
     * @return string
     */
    abstract public function createCopySchemaCommand(string $table): string;

    /**
     * Generate a command for copying chunked table data
     *
     * @param string $table
     * @param int $limit
     * @param int $offset
     * @return string
     */
    abstract public function createCopyChunkedDataCommand(string $table, int $limit, int $offset): string;

    /**
     * Generate a command for copying table data
     *
     * @param string $table
     * @return string
     */
    abstract public function createCopyDataCommand(string $table): string;

    /**
     * Generate a command for copying table triggers
     *
     * @param string $table
     * @return string
     */
    abstract public function createCopyTriggersCommand(string $table): string;
}
<?php

namespace Vmak11\DbCopy\Helpers;

use PDO;
use Vmak11\DbCopy\Config;
use Vmak11\DbCopy\DbHelper;

class MySql extends DbHelper
{
    /**
     * @var PDO
     */
    protected $pdo;

    /**
     * @return array
     */
    public function getTables(): array
    {
        return $this->pdo()->query('show tables')->fetchAll(PDO::FETCH_COLUMN);
    }

    /**
     * Re-usable PDO reference for the read database
     *
     * @return PDO
     */
    protected function pdo(): PDO
    {
        if (empty($this->pdo)) {
            $dsn = "mysql:dbname={$this->readConfig->getDatabase()};port={$this->readConfig->getPort()};host={$this->readConfig->getHost()}";
            $this->pdo = new PDO($dsn, $this->readConfig->getUsername(), $this->readConfig->getPassword());
        }

        return $this->pdo;
    }

    /**
     * @param string $table
     * @return int
     */
    public function getRowCount(string $table): int
    {
        return $this->pdo()->query("SELECT COUNT(1) FROM $table")
            ->fetchAll(PDO::FETCH_COLUMN)[0];
    }

    /**
     * @return string
     */
    public function createDatabaseCommand(): string
    {
        $characterSet = $this->getCharacterSet();
        $collation = $this->getCollation();

        return "mysql {$this->getConnectionString($this->writeConfig)} -e ".
            "'CREATE DATABASE IF NOT EXISTS `{$this->writeConfig->getDatabase()}` CHARACTER SET $characterSet COLLATE $collation;'";
    }

    /**
     * Get the character set from the read database
     *
     * @return string
     */
    protected function getCharacterSet(): string
    {
        return $this->pdo()->query('SELECT @@character_set_database')
            ->fetchAll(PDO::FETCH_COLUMN)[0];
    }

    /**
     * Get the collation from the read database
     *
     * @return string
     */
    protected function getCollation(): string
    {
        return $this->pdo()->query('SELECT @@collation_database')
            ->fetchAll(PDO::FETCH_COLUMN)[0];
    }

    /**
     * @param Config $config
     * @return string
     */
    protected function getConnectionString(Config $config): string
    {
        $port = $config->getPort();
        $host = $config->getHost();
        $username = $config->getUsername();
        $password = $config->getPassword();

        return "--port=$port --host=$host --user=$username --password='$password'";
    }

    /**
     * @param string $table
     * @return string
     */
    public function createCopySchemaCommand(string $table): string
    {
        return "mysqldump {$this->getConnectionString($this->readConfig)} --add-drop-table --create-options --set-charset ".
            "--compress --skip-triggers --no-data {$this->readConfig->getDatabase()} '$table' | ".
            "mysql {$this->getConnectionString($this->writeConfig)} {$this->writeConfig->getDatabase()}";
    }

    /**
     * @param string $table
     * @param int $limit
     * @param int $offset
     * @return string
     */
    public function createCopyChunkedDataCommand(string $table, int $limit, int $offset): string
    {
        return "mysqldump {$this->getConnectionString($this->readConfig)} --single-transaction --extended-insert --disable-keys ".
            "--quick --no-create-info --compress --set-gtid-purged=OFF --where='1 limit $limit offset $offset' {$this->readConfig->getDatabase()} ".
            "'$table' | mysql {$this->getConnectionString($this->writeConfig)} {$this->writeConfig->getDatabase()}";
    }

    /**
     * @param string $table
     * @return string
     */
    public function createCopyDataCommand(string $table): string
    {
        return "mysqldump {$this->getConnectionString($this->readConfig)} --add-locks --single-transaction --extended-insert ".
            "--disable-keys --quick --no-create-info --compress --set-gtid-purged=OFF {$this->readConfig->getDatabase()} ".
            "'$table' | mysql {$this->getConnectionString($this->writeConfig)} {$this->writeConfig->getDatabase()}";
    }

    /**
     * @param string $table
     * @return string
     */
    public function createCopyTriggersCommand(string $table): string
    {
        // Avoid attempting to set the trigger definer to avoid issues if the definer user doesn't exist on the destination database
        return "mysqldump {$this->getConnectionString($this->readConfig)} --compress --no-data --no-create-info --triggers ".
            "{$this->readConfig->getDatabase()} '$table' | sed 's/\sDEFINER=`[^`]*`@`[^`]*`//g' | ".
            "mysql {$this->getConnectionString($this->writeConfig)} {$this->writeConfig->getDatabase()}";
    }
}
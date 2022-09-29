<?php

namespace Vmak11\Tests;

use PDO;
use PHPUnit\Framework\TestCase;
use Vmak11\DbCopy\Config;
use Vmak11\DbCopy\Helpers\MySql;

class MySqlTest extends TestCase
{
    public function testGetTables()
    {
        $mysqlHelper = $this->getMysqlHelper(['pdo']);

        $pdo = $this->getMockBuilder(PDO::class)
            ->disableOriginalConstructor()
            ->setMethods(['query'])
            ->getMock();

        $pdoStatement = $this->getMockBuilder(\PDOStatement::class)
            ->setMethods(['fetchAll'])
            ->getMock();

        $pdoStatement->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_COLUMN)
            ->willReturn([]);
        $pdo->expects($this->once())
            ->method('query')
            ->with('show tables')
            ->willReturn($pdoStatement);
        $mysqlHelper->expects($this->once())
            ->method('pdo')
            ->willReturn($pdo);

        $mysqlHelper->getTables();
    }

    protected function getMysqlHelper(array $methods = [])
    {
        $config = new Config('127.0.0.1', 'test_database', 'test_username', 'test_password');
        $writeConfig = new Config('127.0.0.2', 'test_write_database', 'test_write_username', 'test_write_password',
            3307);

        return $this->getMockBuilder(MySql::class)
            ->setConstructorArgs([$config, $writeConfig])
            ->setMethods($methods)
            ->getMock();
    }

    public function testGetRowCount()
    {
        $mysqlHelper = $this->getMysqlHelper(['pdo']);

        $pdo = $this->getMockBuilder(PDO::class)
            ->disableOriginalConstructor()
            ->setMethods(['query'])
            ->getMock();

        $pdoStatement = $this->getMockBuilder(\PDOStatement::class)
            ->setMethods(['fetchAll'])
            ->getMock();

        $pdoStatement->expects($this->once())
            ->method('fetchAll')
            ->with(PDO::FETCH_COLUMN)
            ->willReturn([27]);
        $pdo->expects($this->once())
            ->method('query')
            ->with('SELECT COUNT(1) FROM table_name')
            ->willReturn($pdoStatement);
        $mysqlHelper->expects($this->once())
            ->method('pdo')
            ->willReturn($pdo);

        $this->assertEquals(27, $mysqlHelper->getRowCount('table_name'));
    }

    public function testCreateDatabaseCommand()
    {
        $mysqlHelper = $this->getMysqlHelper(['pdo']);

        $pdo = $this->getMockBuilder(PDO::class)
            ->disableOriginalConstructor()
            ->setMethods(['query'])
            ->getMock();

        $pdoStatement = $this->getMockBuilder(\PDOStatement::class)
            ->setMethods(['fetchAll'])
            ->getMock();

        $pdoStatement->expects($this->exactly(2))
            ->method('fetchAll')
            ->with(PDO::FETCH_COLUMN)
            ->willReturnOnConsecutiveCalls(['test_character_set'], ['test_collation']);
        $pdo->expects($this->exactly(2))
            ->method('query')
            ->withConsecutive(['SELECT @@character_set_database'], ['SELECT @@collation_database'])
            ->willReturn($pdoStatement);
        $mysqlHelper->expects($this->exactly(2))
            ->method('pdo')
            ->willReturn($pdo);

        $this->assertEquals("mysql --port=3307 --host=127.0.0.2 --user=test_write_username --password='test_write_password' -e ".
            "'CREATE DATABASE IF NOT EXISTS `test_write_database` CHARACTER SET test_character_set COLLATE test_collation;'",
            $mysqlHelper->createDatabaseCommand());
    }

    public function testCreateCopySchemaCommand()
    {
        $mysqlHelper = $this->getMysqlHelper(['pdo']);

        $expected = "mysqldump --port=3306 --host=127.0.0.1 --user=test_username --password='test_password' --add-drop-table --create-options --set-charset ".
            "--compress --skip-triggers --no-data test_database 'test_table' | ".
            "mysql --port=3307 --host=127.0.0.2 --user=test_write_username --password='test_write_password' test_write_database";

        $this->assertEquals($expected, $mysqlHelper->createCopySchemaCommand('test_table'));
    }

    public function testCreateCopyChunkedDataCommand()
    {
        $mysqlHelper = $this->getMysqlHelper(['pdo']);

        $expected = "mysqldump --port=3306 --host=127.0.0.1 --user=test_username --password='test_password' --single-transaction --extended-insert --disable-keys ".
            "--quick --no-create-info --compress --set-gtid-purged=OFF --where='1 limit 1000 offset 3000' test_database ".
            "'test_table' | mysql --port=3307 --host=127.0.0.2 --user=test_write_username --password='test_write_password' test_write_database";

        $this->assertEquals($expected, $mysqlHelper->createCopyChunkedDataCommand('test_table', 1000, 3000));
    }

    public function testCreateCopyDataCommand()
    {
        $mysqlHelper = $this->getMysqlHelper(['pdo']);

        $expected = "mysqldump --port=3306 --host=127.0.0.1 --user=test_username --password='test_password' --add-locks --single-transaction --extended-insert ".
            "--disable-keys --quick --no-create-info --compress --set-gtid-purged=OFF test_database ".
            "'test_table' | mysql --port=3307 --host=127.0.0.2 --user=test_write_username --password='test_write_password' test_write_database";

        $this->assertEquals($expected, $mysqlHelper->createCopyDataCommand('test_table'));
    }

    public function testCreateCopyTriggersCommand()
    {
        $mysqlHelper = $this->getMysqlHelper(['pdo']);

        $expected = "mysqldump --port=3306 --host=127.0.0.1 --user=test_username --password='test_password' --compress --no-data --no-create-info --triggers ".
            "test_database 'test_table' | sed 's/\sDEFINER=`[^`]*`@`[^`]*`//g' | ".
            "mysql --port=3307 --host=127.0.0.2 --user=test_write_username --password='test_write_password' test_write_database";

        $this->assertEquals($expected, $mysqlHelper->createCopyTriggersCommand('test_table'));
    }
}
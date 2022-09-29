<?php
require 'vendor/autoload.php';

use Vmak11\DbCopy\Config;
use Vmak11\DbCopy\Copier;
use Vmak11\DbCopy\Helpers\MySql;
use Vmak11\DbCopy\Processor;

$processor = new Processor(8);
$readConfig = new Config('localhost', 'database_name', 'root', 'password');
$writeConfig = new Config('localhost', 'database_name_copy', 'root', 'password');
$helper = new MySql($readConfig, $writeConfig);
$copier = new Copier($processor, $helper);
$copier->setRowLimit(10000);
$copier->copyAll();


<?php

use \MySQLReplication\Event\DTO\WriteRowsDTO;
use \MySQLReplication\Event\DTO\QueryDTO;
use \MySQLReplication\Event\DTO\UpdateRowsDTO;
use \MySQLReplication\Event\DTO\DeleteRowsDTO;

/**
 *
 *
 * User: zouyi
 * Date: 2017-12-24 09:27
 */
class TestUtils extends \PHPUnit\Framework\TestCase
{
    public function testValidDateTime()
    {
        $this->assertTrue(is_valid_datetime('2015-12-12 12:12:12'));
        $this->assertTrue(is_valid_datetime('2015-12-12 12:12'));
        $this->assertTrue(is_valid_datetime('2015-12-12'));
        $this->assertTrue(is_valid_datetime(null));
    }

    public function testCompareItems()
    {
        $this->assertEquals(compare_items('data', '12345'), '`data`=%s');
        $this->assertEquals(compare_items('data', null), '`data` IS %s');
    }

    public function testGenerateSqlPattern()
    {
        $row = ['values' => ['data' => 'hello', 'id' => 1]];
        $mockWriteEvent = $this->createMock(WriteRowsDTO::class);
        $mockWriteEvent->schema = 'test';
        $mockWriteEvent->table = 'tbl';
        $mockWriteEvent->primary_key = 'id';
        $pattern = generate_sql_pattern($mockWriteEvent, $row, false, false);
        $this->assertEquals($pattern, [
            'values'   => ['hello', 1],
            'template' => 'INSERT INTO `test`.`tbl`(`data`, `id`) VALUES (%s, %s);'
        ]);
        $pattern = generate_sql_pattern($mockWriteEvent, $row, true, false);
        $this->assertEquals($pattern, [
            'values'   => ['hello', 1],
            'template' => 'DELETE FROM `test`.`tbl` WHERE `data`=%s AND `id`=%s LIMIT 1;'
        ]);

        $pattern = generate_sql_pattern($mockWriteEvent, $row, false, true);
        $this->assertEquals($pattern, [
            'values'   => ['hello', 1],
            'template' => 'INSERT INTO `test`.`tbl`(`data`) VALUES (%s);'
        ]);
    }
}

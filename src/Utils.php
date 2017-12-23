<?php
use \MySQLReplication\Event\DTO\WriteRowsDTO;
use \MySQLReplication\Event\DTO\QueryDTO;
use \MySQLReplication\Event\DTO\UpdateRowsDTO;
use \MySQLReplication\Event\DTO\DeleteRowsDTO;

function is_valid_datetime($string)
{
    if (strtotime(($string)) > 0) {
        return true;
    }

    return false;
}

function create_unique_file($filename)
{
    $version = 0;
    $resultFile = $filename;

    while (file_exists($resultFile) && $version < 1000) {
        $resultFile = $filename . '.' . (string)$version;
        $version += 1;
    }

    # if we have to try more than 1000 times, something is seriously wrong
    if ($version >= 1000) {
        throw new Exception('cannot create unique file ' . $filename . '.[0-1000]');
    }

    return $resultFile;
}

function parse_args($args)
{

}

function command_line_args($args)
{

}

function compare_items($k, $v)
{
    #caution: if v is NULL, may need to process
    if ($v == null) {
        return "`{$k}` IS NULL";
    }

    return "`{$k}`=$v";
}

function fix_object($value)
{
    return $value;
}

function concat_sql_from_binlogevent($cursor, $binlogevent, $row = null, $eStartPos = null, $flashback = false, $nopk = false)
{
    if ($flashback && $nopk) {
        throw new Exception('only one of flashback or nopk can be True');
    }
    if (! (
        ($binlogevent instanceof WriteRowsDTO)
        || ($binlogevent instanceof UpdateRowsDTO)
        || ($binlogevent instanceof DeleteRowsDTO)
        || ($binlogevent instanceof QueryDTO)
    )
    ) {
        throw new Exception('binlogevent must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent');
    }

    $sql = '';

    if (($binlogevent instanceof WriteRowsDTO)
        || ($binlogevent instanceof UpdateRowsDTO)
        || ($binlogevent instanceof DeleteRowsDTO)
    ) {
        $pattern = generate_sql_pattern($binlogevent, $row, $flashback, $nopk);
        $sql = $cursor->mogrify($pattern['template'], $pattern['values']);
        $sql += printf(
            ' #start %s end %s time %s',
            $eStartPos,
            $binlogevent->packet->log_pos,
            date_from_timestamp($binlogevent->timestamp)
        );
    } elseif (! $flashback
        && ($binlogevent instanceof QueryDTO)
        && $binlogevent->getQuery() != 'BEGIN'
        && $binlogevent->getQuery() != 'COMMIT'
    ) {
        if ($binlogevent->schema) {
            $sql = "USE " . $binlogevent->schema . ";\n";
        }
        $sql += fix_object($binlogevent->getQuery()) . ";";

    }

    return $sql;
}

function date_from_timestamp($timestamp)
{
    return date($timestamp, 'Y-m-d H:i:s');
}

function generate_sql_pattern($binlogevent, $row = null, $flashback = false, $nopk = false)
{
    $template = '';
    $values = [];
    if ($flashback) {
        if ($binlogevent instanceof WriteRowsDTO) {

            $arr = [];
            $values = array_values($row['values']);
            foreach ($row['values'] as $k => $v) {
                $arr[] = compare_items($k, $v);
            }
            $template = printf(
                'DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;',
                $binlogevent->schema,
                $binlogevent->table,
                implode(' AND ', $arr)
            );
        } elseif ($binlogevent instanceof DeleteRowsDTO) {

            $arr = [];
            $keyArr = [];
            $values = array_values($row['values']);
            foreach ($row['values'] as $k => $v) {
                $keyArr[] = '`' . $k . '`';
                $arr[] = "'{$v}'";
            }
            $whereString = implode(', ', $arr);
            $template = printf(
                "INSERT INTO `%s`.`%s`(%s) VALUES (%s);",
                $binlogevent->schema,
                $binlogevent->table,
                implode(', ', $keyArr),
                implode(', ', $arr)
            );
        } elseif ($binlogevent instanceof UpdateRowsDTO) {
            $arr = [];
            $keyArr = [];
            $values = $row['values'];
            foreach ($row['before_values'] as $k => $v) {
                $keyArr[] = '`' . $k . '`=' . $v;
            }
            foreach ($row['after_values'] as $k => $v) {
                $arr[] = compare_items($k, $v);
            }
            $whereString = implode(', ', $arr);
            $template = printf(
                "UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;",
                $binlogevent->schema,
                $binlogevent->table,
                implode(', ', $keyArr),
                implode(' AND ', $arr)
            );
            $values = array_values($row['before_values'] + $row['after_values']);
        }
    } else {

        if ($binlogevent instanceof WriteRowsDTO) {

//            if ($nopk) {
//                if ($binlogevent->primary_key) {
//                    array_pop($row['values']);
//                }
//            }
            $arr = [];
            $keyArr = [];
            $values = array_values($row['values']);
            foreach ($row['values'] as $k => $v) {
                $keyArr[] = '`' . $k . '`';
                $arr[] = "'{$v}'";
            }
            $whereString = implode(', ', $arr);
            $template = printf(
                "INSERT INTO `%s`.`%s`(%s) VALUES (%s);",
                $binlogevent->schema,
                $binlogevent->table,
                implode(', ', $keyArr),
                implode(', ', $arr)
            );
        } elseif ($binlogevent instanceof DeleteRowsDTO) {

            $arr = [];
            $values = array_values($row['values']);
            foreach ($row['values'] as $k => $v) {
                $arr[] = compare_items($k, $v);
            }
            $template = printf(
                'DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;',
                $binlogevent->schema,
                $binlogevent->table,
                implode(' AND ', $arr)
            );
        } elseif ($binlogevent instanceof UpdateRowsDTO) {
            $arr = [];
            $keyArr = [];
            foreach ($row['before_values'] as $k => $v) {
                $keyArr[] = '`' . $k . '`=' . $v;
            }
            foreach ($row['after_values'] as $k => $v) {
                $arr[] = compare_items($k, $v);
            }
            $template = printf(
                "UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;",
                $binlogevent->schema,
                $binlogevent->table,
                implode(', ', $keyArr),
                implode(' AND ', $arr)
            );
            $values = array_values($row['after_values'] + $row['before_values']);
        }
    }

    return [
        'template' => $template,
        'values'   => $values,
    ];
}

function reversed_lines($file)
{
    $part = '';
    while ($block = reversed_blocks($file)) {

    }
}

function reversed_blocks($file, $blockSize = 4096)
{

    fseek($file, 0, SEEK_END);
    $here = ftell($file);
    while ($here > 0) {
        $delta = min($blockSize, $here);
        $here -= $delta;
        fseek($here, SEEK_SET);
        yield fread($file, $delta);
    }
}

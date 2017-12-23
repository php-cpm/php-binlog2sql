<?php

namespace Binlog2Sql;

use MySQLReplication\Config\ConfigBuilder;
use MySQLReplication\Event\DTO\DeleteRowsDTO;
use MySQLReplication\Event\DTO\FormatDescriptionEventDTO;
use MySQLReplication\Event\DTO\QueryDTO;
use MySQLReplication\Event\DTO\RotateDTO;
use MySQLReplication\Event\DTO\UpdateRowsDTO;
use MySQLReplication\Event\DTO\WriteRowsDTO;
use MySQLReplication\MySQLReplicationFactory;
use \Exception;

/**
 *
 *
 * User: zouyi
 * Date: 2017-11-28 21:30
 */
class Binlog2Sql
{

    public $connectionSettings;
    public $startFile;
    public $startPos;
    public $endFile;
    public $endPos;
    public $startTime;
    public $stopTime;

    public $onlySchemas;
    public $onlyTables;

    public $nopk;
    public $flashback;
    public $stopnever;

    public $binlogList;

    public $connection;

    public $eofFile;
    public $eofPos;
    public $serverId;

    public function __contruct(
        $connectionSettings,
        $startFile = '',
        // use binlog v4
        $startPos = '4',
        $endFile = '',
        $endPos = '',
        $startTime = '',
        $stopTime = '',
        $onlySchemas = '',
        $onlyTables = '',
        $nopk = false,
        $flashback = false,
        $stopnever = false
    ) {
        if (! $startFile) {
            throw new \Exception('lack of parameter,startFile.');
        }
        $this->connectionSettings = $connectionSettings ? $connectionSettings : [
            'host'   => '127.0.0.1',
            'user'   => 'slave',
            'passwd' => 'slave',
            'port'   => '3306',
        ];
        $this->startFile = $startFile;
        $this->startPos = $startPos;
        $this->endFile = $endFile ? $endFile : $startFile;
        $this->endPos = $endPos;
        $this->startTime = $startTime ? date('Y-m-d H:i:s', strtotime($startTime))
            : date('Y-m-d H:i:s', strtotime('1970-01-01 00:00:00'));
        $this->stopTime = $stopTime ? date('Y-m-d H:i:s', strtotime($stopTime))
            : date('Y-m-d H:i:s', strtotime('1970-01-01 00:00:00'));
        $this->onlySchemas = $onlySchemas;
        $this->onlyTables = $onlyTables;

        $this->nopk = $nopk;
        $this->flashback = $flashback;
        $this->stopnever = $stopnever;

        $this->binlogList = [];
        $this->connection = mysqli_connect(
            $this->connectionSettings['host'],
            $this->connectionSettings['user'],
            $this->connectionSettings['passwd'],
            $this->connectionSettings['port']
        );

        $this->getBinlogList();
    }

    public function getBinlogList()
    {
        try {
            $query = "SHOW MASTER STATUS";
            $cursor = mysqli_query($this->connection, $query);
            $result = mysqli_fetch_assoc($cursor);
            $this->eofFile = $result['File'];
            $this->eofPos = $result['Position'];


            $query = "SHOW MASTER LOGS";
            $cursor = mysqli_query($this->connection, $query);
            $binIndex = [];
            while ($row = mysqli_fetch_assoc($cursor)) {
                $binIndex[] = $row['Log_name'];
            }

            if (! in_array($this->startFile, $binIndex)) {
                throw new Exception(printf('parameter error: startFile %s not in mysql server', $this->startFile));
            }

            foreach ($binIndex as $bin) {
                $offset = explode('.', $bin)[1];
                $start_offset = explode('.', $this->startFile)[1];
                $end_offset = explode('.', $this->endFile)[1];
                if ($offset >= $start_offset && $offset <= $end_offset) {
                    $this->binlogList[] = $bin;
                }
            }

            $query = "SELECT @@server_id";
            $cursor = mysqli_query($this->connection, $query);
            $result = mysqli_fetch_assoc($cursor);
            if (! isset($result['@@server_id'])) {
                $message = printf(
                    'need set server_id in mysql server %s:%s',
                    $this->connectionSettings['host'],
                    $this->connectionSettings['port']
                );
                throw new Exception($message);
            }
            $this->serverId = $result['@@server_id'];
        } finally {
            mysqli_close($this->connection);
        }
    }

    public function processBinlog()
    {
        $config = (new ConfigBuilder())
            ->withHost($this->connectionSettings['host'])
            ->withPort($this->connectionSettings['port'])
            ->withUser($this->connectionSettings['user'])
            ->withPassword($this->connectionSettings['passwd'])
            ->withSlaveId($this->serverId)
            ->withBinLogPosition($this->startPos)
            ->withBinLogFileName($this->startFile)
            ->withTablesOnly($this->onlyTables)
            ->withDatabasesOnly($this->onlySchemas)
            ->build();
        $stream = new MySQLReplicationFactory();

        $stream->consume();
        $cur = $this->connection;
        $tmpFile = create_unique_file($this->connectionSettings['host'] . '.' . $this->connectionSettings['port']);
        $ftmp = fopen($tmpFile, "w");
        $flagLastEvent = false;
        $eStartPos = null;
        $lastPos = null;

        try {
            foreach ($stream as $binlogevent) {
                if (! $this->stopnever) {
                    if (($config::getBinLogFileName() == $this->endFile
                            && $config::getBinLogPosition() == $this->endPos)
                        || ($config::getBinLogFileName() == $this->eofFile
                            && $config::getBinLogPosition() == $this->eofPos)
                    ) {
                        $flagLastEvent = true;
                    } elseif ($binlogevent->timestamp < $this->startTime) {
                        if (! ($binlogevent instanceof RotateDTO)
                            || $binlogevent instanceof FormatDescriptionEventDTO
                        ) {
                            $lastPos = $binlogevent->getEventInfo()->getPos();
                        }
                        continue;
                    } elseif ((! in_array($config::getBinLogFileName(), $this->binlogList))
                        || ($this->endPos
                            && $config::getBinLogFileName() == $this->endFile
                            && $config::getBinLogPosition() > $this->endPos
                        )
                        || ($config::getBinLogFileName() == $this->endFile
                            && $config::getBinLogPosition() > $this->eofPos
                        )
                        || ($binlogevent->timestamp >= $this->stopTime)
                    ) {
                        break;
                    }
                }

                if ($binlogevent instanceof QueryDTO && $binlogevent->getQuery() == 'BEGIN') {
                    $eStartPos == $lastPos;
                }

                if ($binlogevent instanceof QueryDTO) {
                    $sql = concat_sql_from_binlogevent($cur, $binlogevent, $this->flashback, $this->nopk);
                    if ($sql) {
                        print $sql;
                    }
                } elseif ($binlogevent instanceof WriteRowsDTO
                    || $binlogevent instanceof UpdateRowsDTO
                    || $binlogevent instanceof DeleteRowsDTO
                ) {
                    foreach ($binlogevent->getChangedRows() as $row) {
                        $sql = concat_sql_from_binlogevent($cur, $binlogevent, $row, $this->flashback, $this->nopk);
                        if ($this->flashback) {
                            fwrite($ftmp, $sql + "\n");
                        } else {
                            print $sql;
                        }
                    }
                }
                if (! ($binlogevent instanceof RotateDTO
                    || $binlogevent instanceof FormatDescriptionEventDTO)
                ) {
                    $lastPos = $binlogevent->getEventInfo()->getPos();
                }

                if ($flagLastEvent) {
                    break;
                }
            }
            fclose($ftmp);

        } finally {
            unlink($tmpFile);
        }

        mysqli_close($cur);
        $stream->getDbConnection()->close();

        return true;
    }

    public function test()
    {
    }
}

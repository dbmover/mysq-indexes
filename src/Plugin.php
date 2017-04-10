<?php

namespace Dbmover\Mysql\Indexes;

use Dbmover\Indexes;
use PDO;

class Plugin extends Indexes\Plugin
{
    public function __invoke(string $sql) : string
    {
        // MySQL-style inline index defintions
        if (preg_match_all("@^CREATE TABLE\s+([^\s]+)\s*\(.*?^\s*((UNIQUE)?\s*INDEX\s*\(.*?)\)@ms", $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $name = "{$index[1]}_".preg_replace('@,\s*@', '_', $index[5]).'_idx';
                $this->requestedIndexes[$name] = [
                    "CREATE INDEX {$index[4]} $name ON {$index[1]}({$index[5]})",
                    $index[4],
                    $name,
                    $index[1],
                    '',
                    $index[5],
                ];
                $sql = preg_replace("@{$index[2]},?$@ms", '', $sql);
            }
        }
        return parent::__invoke($sql);
    }

    protected function existingIndexes() : array
    {
        $stmt = $this->loader->getPdo()->prepare(
            "SELECT table_name, column_name, index_name, non_unique, '' AS type
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = ?");
        $stmt->execute([$this->loader->getDatabase()]);
        $existing = [];
        while (false !== ($row = $stmt->fetch(PDO::FETCH_ASSOC))) {
            if ($row['index_name'] == 'PRIMARY') {
                $row['index_name'] = "{$row['table_name']}_PRIMARY";
            }
            if (!isset($existing[$row['index_name']])) {
                $existing[$row['index_name']] = $row;
            } else {
                $existing[$row['index_name']]['column_name'] .= ",{$row['column_name']}";
            }
        }
        return $existing;
    }

    protected function dropIndex(string $index, string $table) : string
    {
        return "DROP INDEX `$index` ON `$table`;";
    }

    protected function dropPrimaryKey(string $index, string $table) : string
    {
        return "DROP INDEX `PRIMARY` ON `$table`;";
    }
}


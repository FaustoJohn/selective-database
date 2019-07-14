<?php

namespace Odan\Database;

use PDO;

final class Connection extends PDO
{
    /**
     * @var Quoter
     */
    protected $quoter;

    /**
     * @return SelectQuery
     */
    public function select(): SelectQuery
    {
        return new SelectQuery($this);
    }

    /**
     * @return InsertQuery
     */
    public function insert(): InsertQuery
    {
        return new InsertQuery($this);
    }

    /**
     * @return UpdateQuery
     */
    public function update(): UpdateQuery
    {
        return new UpdateQuery($this);
    }

    /**
     * @return DeleteQuery
     */
    public function delete(): DeleteQuery
    {
        return new DeleteQuery($this);
    }

    /**
     * Get quoter.
     *
     * @return Quoter
     */
    public function getQuoter(): Quoter
    {
        if ($this->quoter === null) {
            $this->quoter = new Quoter($this);
        }

        return $this->quoter;
    }

    /**
     * Retrieving a list of column values.
     *
     * sample:
     * $lists = $db->queryValues('SELECT id FROM table;', 'id');
     *
     * @param string $sql The sql
     * @param string $key The key
     *
     * @return array The values
     */
    public function queryValues(string $sql, string $key): array
    {
        $result = [];
        $statement = $this->query($sql);
        while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
            $result[] = $row[$key];
        }

        return $result;
    }

    /**
     * Retrieve only the given column of the first result row.
     *
     * @param string $sql
     * @param string $column
     * @param mixed $default
     *
     * @return mixed|null
     */
    public function queryValue(string $sql, string $column, $default = null)
    {
        $result = $default;
        if ($row = $this->query($sql)->fetch(PDO::FETCH_ASSOC)) {
            $result = $row[$column];
        }

        return $result;
    }

    /**
     * Map query result by column as new index.
     *
     * <code>
     * $rows = $db->queryMapColumn('SELECT * FROM table;', 'id');
     * </code>
     *
     * @param string $sql
     * @param string $key Column name to map as index
     *
     * @return array
     */
    public function queryMapColumn(string $sql, string $key): array
    {
        $result = [];
        $statement = $this->query($sql);
        while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
            $result[$row[$key]] = $row;
        }

        return $result;
    }
}

<?php

namespace Selective\Database;

use Closure;
use Exception;
use InvalidArgumentException;

/**
 * Select Query.
 */
final class SelectQueryBuilder implements QueryInterface
{
    /**
     * @var Connection
     */
    private Connection $connection;

    /**
     * @var Quoter
     */
    private Quoter $quoter;

    /**
     * Constructor.
     *
     * @param Connection $connection The connection
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->quoter = $connection->getQuoter();
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param array $options The options
     *
     * @return array The sql
     */
    public function getSelectSql(array $sql, array $options = []): array
    {
        $sql[] = trim('SELECT ' . trim(implode(' ', $options)));

        return $sql;
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param array $inputColumns The input columns
     *
     * @return array The sql
     */
    public function getColumnsSql(array $sql, array $inputColumns): array
    {
        if (empty($inputColumns)) {
            $sql[] = '*';

            return $sql;
        }
        $columns = [];
        foreach ($inputColumns as $key => $column) {
            if ($column instanceof Closure) {
                // Sub Select
                $query = new SelectQuery($this->connection);
                $column($query);
                $column = new RawExp($query->build(false));
            }

            if (!is_int($key)) {
                // Column with alias as array
                $column = [$key => $column];
            }

            $columns[] = $column;
        }
        $sql[] = implode(',', $this->quoter->quoteNames($columns));

        return $sql;
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param Closure|RawExp|string $from The table
     *
     * @return array The sql
     */
    public function getFromSql(array $sql, Closure|RawExp|string $from): array
    {
        if (empty($from)) {
            return $sql;
        }

        $sql[] = 'FROM ' . $this->processFromTable($from);

        return $sql;
    }

    /**
     * @param Closure|RawExp|string $table The from table
     * @return string
     */
    private function processFromTable(Closure|RawExp|string $table): string
    {
        if (gettype($table) !== "object") {
            return $this->quoter->quoteName($table);
        }

        return match ($table::class) {
            Closure::class => $this->getSubSelectSql($table),
            RawExp::class => $table->getValue(),
            default => throw new InvalidArgumentException("table must either one of the following: Closure | RawExp | array | string")
        };
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param array|null $join The join
     *
     * @return array The sql
     * @throws Exception
     */
    public function getJoinSql(array $sql, array $join = null): array
    {
        if (empty($join)) {
            return $sql;
        }
        foreach ($join as $item) {
            [$type, $table, $leftField, $operator, $rightField] = $item;
            $joinType = strtoupper($type) . ' JOIN';


            $table = $this->processJoinTable($table);
            if ($leftField instanceof RawExp) {
                $sql[] = sprintf('%s %s ON (%s)', $joinType, $table, $leftField->getValue());
            } else {
                $leftField = $this->quoter->quoteName($leftField);
                $rightField = $this->quoter->quoteName($rightField);
                $sql[] = sprintf('%s %s ON %s %s %s', $joinType, $table, $leftField, $operator, $rightField);
            }
        }

        return $sql;
    }

    /**
     * Allows join statements to be defined as a RawExp object or a Closure
     * @param Closure|RawExp|array|string $table The table
     * @return string
     */
    private function processJoinTable(Closure|RawExp|array|string $table): string
    {
        if (gettype($table) !== "object") {
            return $this->quoter->quoteName($table);
        }

        return match ($table::class) {
            Closure::class => $this->getSubSelectSql($table),
            RawExp::class => $table->getValue(),
            default => throw new InvalidArgumentException("table must be of type: Closure|RawExp|array|string")
        };
    }

    /**
     * @param Closure $callable The callback function
     * @return string
     */
    private function getSubSelectSql(Closure $callable): string
    {
        $query = new SelectQuery($this->connection);
        $callable($query);
        return $query->build(false);
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param array $groupBy The group by
     *
     * @return array The sql
     */
    public function getGroupBySql(array $sql, array $groupBy): array
    {
        if (empty($groupBy)) {
            return $sql;
        }
        $sql[] = 'GROUP BY ' . implode(', ', $this->quoter->quoteByFields($groupBy));

        return $sql;
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param array|null $orderBy The order
     *
     * @return array The sql
     */
    public function getOrderBySql(array $sql, array $orderBy = null): array
    {
        if (empty($orderBy)) {
            return $sql;
        }
        $sql[] = 'ORDER BY ' . implode(', ', $this->quoter->quoteByFields($orderBy));

        return $sql;
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param int|null $limit The limit
     * @param int|null $offset The offset
     *
     * @return array The sql
     */
    public function getLimitSql(array $sql, int $limit = null, int $offset = null): array
    {
        if (!isset($limit)) {
            return $sql;
        }
        if ($offset > 0) {
            $sql[] = sprintf('LIMIT %s OFFSET %s', $limit, $offset);
        } else {
            $sql[] = sprintf('LIMIT %s', $limit);
        }

        return $sql;
    }

    /**
     * Get sql.
     *
     * @param array $sql The sql
     * @param array|null $unions The unions
     *
     * @return array The sql
     */
    public function getUnionSql(array $sql, array $unions = null): array
    {
        if (empty($unions)) {
            return $sql;
        }
        foreach ($unions as $union) {
            $sql[] = 'UNION ' . trim($union[0] . ' ' . $union[1]);
        }

        return $sql;
    }

    /**
     * Get sql.
     *
     * @param string $sql The sql
     * @param string|null $alias The alias
     *
     * @return string $sql The sql
     */
    public function getAliasSql(string $sql, string $alias = null): string
    {
        if (!isset($alias)) {
            return $sql;
        }

        return sprintf('(%s) AS %s', $sql, $this->quoter->quoteName($alias));
    }
}

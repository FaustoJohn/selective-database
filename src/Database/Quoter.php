<?php

namespace Odan\Database;

/**
 * Quoter
 */
class Quoter
{
    /**
     * Connection
     *
     * @var Connection
     */
    protected $pdo;

    /**
     * Constructor.
     *
     * @param Connection $pdo
     */
    public function __construct(Connection $pdo)
    {
        $this->pdo = $pdo;
    }

    /**
     * Quotes a value for use in a query.
     *
     * @param mixed $value
     * @return string|false a quoted string
     */
    public function quoteValue($value): string
    {
        if ($value === null) {
            return 'NULL';
        }
        return $this->pdo->quote($value);
    }

    /**
     * Quote array values.
     *
     * @param array|null $array
     * @return array
     */
    public function quoteArray(array $array): array
    {
        if (empty($array)) {
            return [];
        }
        foreach ($array as $key => $value) {
            $array[$key] = $this->quoteValue($value);
        }
        return $array;
    }

    /**
     * Escape identifier (column, table) with backticks
     *
     * @see: http://dev.mysql.com/doc/refman/5.0/en/identifiers.html
     *
     * @param string $identifier Identifier name
     * @return string Quoted identifier
     */
    public function quoteName(string $identifier): string
    {
        $identifier = trim($identifier);
        $separators = array(' AS ', ' ', '.');
        foreach ($separators as $sep) {
            $pos = strripos($identifier, $sep);
            if ($pos) {
                return $this->quoteNameWithSeparator($identifier, $sep, $pos);
            }
        }
        return $this->quoteIdentifier($identifier);
    }

    /**
     * Quote array of names.
     *
     * @param array $identifiers
     * @return array
     */
    public function quoteNames(array $identifiers): array
    {
        foreach ($identifiers as $key => $identifier) {
            if ($identifier instanceof RawExp) {
                $identifiers[$key] = $identifier->getValue();
                continue;
            }
            $identifiers[$key] = $this->quoteName($identifier);
        }
        return $identifiers;
    }

    /**
     * Quotes an identifier that has a separator.
     *
     * @param string $spec The identifier name to quote.
     * @param string $sep The separator, typically a dot or space.
     * @param int $pos The position of the separator.
     * @return string The quoted identifier name.
     */
    protected function quoteNameWithSeparator(string $spec, string $sep, int $pos): string
    {
        $len = strlen($sep);
        $part1 = $this->quoteName(substr($spec, 0, $pos));
        $part2 = $this->quoteIdentifier(substr($spec, $pos + $len));
        return "{$part1}{$sep}{$part2}";
    }

    /**
     * Quotes an identifier name (table, index, etc); ignores empty values and
     * values of '*'.
     *
     * Escape backticks inside by doubling them
     * Enclose identifier in backticks
     *
     * After such formatting, it is safe to insert the $table variable into query.
     *
     * @param string $name The identifier name to quote.
     * @return string The quoted identifier name.
     * @see quoteName()
     */
    public function quoteIdentifier(string $name): string
    {
        $name = trim($name);
        if ($name == '*') {
            return $name;
        }
        return "`" . str_replace("`", "``", $name) . "`";
    }

    /**
     * Quote Set values.
     *
     * @param array $row A row
     * @return string Sql string
     */
    public function quoteSetValues(array $row): string
    {
        $values = [];
        foreach ($row as $key => $value) {
            if ($value instanceof RawExp) {
                $values[] = $this->quoteName($key) . '=' . $value->getValue();
                continue;
            }
            $values[] = $this->quoteName($key) . '=' . $this->quoteValue($value);
        }
        return implode(', ', $values);
    }

    /**
     * Quote bulk values.
     *
     * @param array $row A row
     * @return string Sql string
     */
    public function quoteBulkValues(array $row): string
    {
        $values = [];
        foreach ($row as $key => $value) {
            $values[] = $this->quoteValue($value);
        }
        return implode(',', $values);
    }

    /**
     * Quote fields values.
     *
     * @param array $row A row
     * @return string Sql string
     */
    public function quoteFields(array $row): string
    {
        $fields = [];
        foreach (array_keys($row) as $field) {
            $fields[] = $this->quoteName($field);
        }
        return implode(', ', $fields);
    }

    /**
     * Get sql.
     *
     * @param $identifiers
     * @return array
     */
    public function quoteByFields($identifiers): array
    {
        foreach ((array)$identifiers as $key => $identifier) {
            if ($identifier instanceof RawExp) {
                $identifiers[$key] = $identifier->getValue();
                continue;
            }
            // table.id ASC
            // @todo Fix it
            if (preg_match('/^([\w-\.]+)(\s)*(.*)$/', $identifier, $match)) {
                $identifiers[$key] = $this->quoteIdentifier($match[1]) . $match[2] . $match[3];
                continue;
            }
            $identifiers[$key] = $this->quoteName($identifier);
        }
        return $identifiers;
    }
}

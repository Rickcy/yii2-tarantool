<?php


namespace rickcy\tarantool;


use Tarantool\Client\Keys;
use Tarantool\Client\Request\ExecuteRequest;
use Tarantool\Client\SqlQueryResult;
use Tarantool\Client\SqlUpdateResult;
use yii\base\Component;
use yii\db\Expression;

/**
 * @property string $rawSql
 * @property string $sql
 */
class Command extends Component
{

    /**
     * @var Connection the DB connection that this command is associated with
     */
    public $db;
    /**
     * @var array the parameters (name => value) that are bound to the current PDO statement.
     * This property is maintained by methods such as [[bindValue()]]. It is mainly provided for logging purpose
     * and is used to generate [[rawSql]]. Do not modify it directly.
     */
    public $params = [];

    /**
     * @var string the SQL statement that this command represents
     */
    private $_sql;
    private $_pendingParams;

    /**
     * Returns the SQL statement for this command.
     *
     * @return string the SQL statement to be executed
     */
    public function getSql() : string
    {
        return $this->_sql;
    }

    /**
     * Specifies the SQL statement to be executed. The SQL statement will be quoted using [[Connection::quoteSql()]].
     * The previous SQL (if any) will be discarded, and [[params]] will be cleared as well. See [[reset()]]
     * for details.
     *
     * @param string $sql the SQL statement to be set.
     *
     * @return Command this command instance
     * @see reset()
     * @see cancel()
     */
    public function setSql($sql) : Command
    {
        if ($sql !== $this->_sql) {
            $this->reset();
            $this->_sql = $this->db->quoteSql($sql);
        }

        return $this;
    }


    /**
     * Resets command properties to their initial state.
     *
     * @since 2.0.13
     */
    protected function reset()
    {
        $this->_sql = null;
        $this->params = [];
    }


    /**
     * Binds a list of values to the corresponding parameters.
     * This is similar to [[bindValue()]] except that it binds multiple values at a time.
     * Note that the SQL data type of each value is determined by its PHP type.
     *
     * @param array $values the values to be bound. This must be given in terms of an associative
     * array with array keys being the parameter names, and array values the corresponding parameter values,
     * e.g. `[':name' => 'John', ':age' => 25]`. By default, the PDO type of each value is determined
     * by its PHP type. You may explicitly specify the PDO type by using a [[yii\db\PdoValue]] class: `new PdoValue(value, type)`,
     * e.g. `[':name' => 'John', ':profile' => new PdoValue($profile, \PDO::PARAM_LOB)]`.
     *
     * @return $this the current command being executed
     */
    public function bindValues($values) : self
    {
        if (empty($values)) {
            return $this;
        }

        foreach ($values as $name => $value) {
            $this->_pendingParams[$name] = $value;
            $this->params[$name] = $value;
        }

        return $this;
    }

    public function queryAll() : SqlQueryResult
    {
        $this->db->open();
        return $this->executeQuery($this->getRawSql());
    }

    /**
     * Specifies the SQL statement to be executed. The SQL statement will not be modified in any way.
     * The previous SQL (if any) will be discarded, and [[params]] will be cleared as well. See [[reset()]]
     * for details.
     *
     * @param string $sql the SQL statement to be set.
     *
     * @return Command this command instance
     * @since 2.0.13
     * @see reset()
     * @see cancel()
     */
    public function setRawSql($sql) : Command
    {
        if ($sql !== $this->_sql) {
            $this->reset();
            $this->_sql = $sql;
        }

        return $this;
    }


    /**
     * Returns the raw SQL by inserting parameter values into the corresponding placeholders in [[sql]].
     * Note that the return value of this method should mainly be used for logging purpose.
     * It is likely that this method returns an invalid SQL due to improper replacement of parameter placeholders.
     *
     * @return string the raw SQL with parameter values inserted into the corresponding placeholders in [[sql]].
     */
    public function getRawSql() : string
    {
        if (empty($this->params)) {
            return $this->_sql;
        }
        $params = [];
        foreach ($this->params as $name => $value) {
            if (is_string($name) && strncmp(':', $name, 1)) {
                $name = ':' . $name;
            }
            if (is_string($value)) {
                $params[$name] = $this->db->quoteValue($value);
            } elseif (is_bool($value)) {
                $params[$name] = ($value ? 'TRUE' : 'FALSE');
            } elseif ($value === null) {
                $params[$name] = 'NULL';
            } elseif ((!is_object($value) && !is_resource($value)) || $value instanceof Expression) {
                $params[$name] = $value;
            }
        }
        if (!isset($params[1])) {
            return strtr($this->_sql, $params);
        }
        $sql = '';
        foreach (explode('?', $this->_sql) as $i => $part) {
            $sql .= ($params[$i] ?? '') . $part;
        }

        return $sql;
    }


    private function executeQuery(string $sql) : SqlQueryResult
    {
        $request = new ExecuteRequest($sql);
        $response = $this->db->handler->handle($request);

        return new SqlQueryResult(
            $response->getBodyField(Keys::DATA),
            $response->getBodyField(Keys::METADATA)
        );
    }

    /**
     * @return \Tarantool\Client\SqlQueryResult
     */
    public function query() : SqlQueryResult
    {
        $this->db->open();
        return $this->executeQuery($this->getRawSql());
    }

    /**
     * @return \Tarantool\Client\SqlUpdateResult
     */
    public function execute() : SqlUpdateResult
    {
        $this->db->open();

        return $this->executeUpdate($this->getRawSql());
    }

    private function executeUpdate(string $sql) : SqlUpdateResult
    {
        $request = new ExecuteRequest($sql);

        return new SqlUpdateResult(
            $this->db->handler->handle($request)->getBodyField(Keys::SQL_INFO)
        );
    }


}
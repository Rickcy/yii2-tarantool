<?php


namespace rickcy\tarantool;


use InvalidArgumentException;
use ReflectionClass;
use Yii;
use yii\base\InvalidConfigException;
use yii\db\ActiveQueryInterface;
use yii\db\BaseActiveRecord;
use yii\db\Query;
use yii\helpers\ArrayHelper;
use yii\helpers\Inflector;
use yii\helpers\StringHelper;

abstract class ActiveRecord extends BaseActiveRecord
{


    /**
     * @var array attribute values indexed by attribute names
     */
    private $_attributes = [];
    /**
     * @var array|null old attribute values indexed by attribute names.
     * This is `null` if the record [[isNewRecord|is new]].
     */
    private $_oldAttributes;
    /**
     * @var array related models indexed by the relation names
     */
    private $_related = [];
    /**
     * @var array relation names indexed by their link attributes
     */
    private $_relationsDependencies = [];

    /**
     * Creates an [[ActiveQuery]] instance with a given SQL statement.
     * Note that because the SQL statement is already specified, calling additional
     * query modification methods (such as `where()`, `order()`) on the created [[ActiveQuery]]
     * instance will have no effect. However, calling `with()`, `asArray()` or `indexBy()` is
     * still fine.
     * Below is an example:
     * ```php
     * $customers = Customer::findBySql('SELECT * FROM customer')->all();
     * ```
     *
     * @param string $sql the SQL statement to be executed
     * @param array $params parameters to be bound to the SQL statement during execution.
     *
     * @return \rickcy\tarantool\ActiveQuery the newly created [[ActiveQuery]] instance
     * @throws \Exception
     */
    public static function findBySql($sql, $params = []) : ActiveQuery
    {
        $query = static::find();
        $query->sql = $sql;

        return $query->params($params);
    }

    /**
     * {@inheritdoc}
     * @return \rickcy\tarantool\ActiveQuery the newly created [[ActiveQuery]] instance.
     * @throws \Exception
     */
    public static function find() : ActiveQueryInterface
    {
        return Yii::createObject(ActiveQuery::class, [static::class]);
    }


    /**
     * {@inheritdoc}
     */
    public static function populateRecord($record, $row)
    {
        $columns = $record->attributes();
        foreach ($row as $name => $value) {
            if (isset($columns[$name])) {
                $row[$name] = $value;
            }
        }
        parent::populateRecord($record, $row);
    }

    /**
     * Finds ActiveRecord instance(s) by the given condition.
     * This method is internally called by [[findOne()]] and [[findAll()]].
     *
     * @param mixed $condition please refer to [[findOne()]] for the explanation of this parameter
     *
     * @return ActiveQueryInterface the newly created [[ActiveQueryInterface|ActiveQuery]] instance.
     * @throws \Exception
     * @internal
     */
    protected static function findByCondition($condition) : ActiveQueryInterface
    {
        $query = static::find();

        if (!ArrayHelper::isAssociative($condition)) {
            // query by primary key
            $primaryKey = static::primaryKey();
            if (isset($primaryKey[0])) {
                $pk = $primaryKey[0];
                if (!empty($query->join) || !empty($query->joinWith)) {
                    $pk = static::tableName() . '.' . $pk;
                }
                // if condition is scalar, search for a single primary key, if it is array, search for multiple primary key values
                $condition = [$pk => is_array($condition) ? array_values($condition) : $condition];
            } else {
                throw new InvalidConfigException('"' . static::class . '" must have a primary key.');
            }
        } elseif (is_array($condition)) {
            $aliases = static::filterValidAliases($query);
            $condition = static::filterCondition($condition, $aliases);
        }

        return $query->andWhere($condition);
    }

    /**
     * Returns the primary key name(s) for this AR class.
     * The default implementation will return the primary key(s) as declared
     * in the DB table that is associated with this AR class.
     * If the DB table does not declare any primary key, you should override
     * this method to return the attributes that you want to use as primary keys
     * for this AR class.
     * Note that an array should be returned even for a table with single primary key.
     *
     * @return string[] the primary keys of the associated database table.
     */
    public static function primaryKey() : array
    {
        return ['id'];
    }

    /**
     * Declares the name of the database table associated with this AR class.
     * By default this method returns the class name as the table name by calling [[Inflector::camel2id()]]
     * with prefix [[Connection::tablePrefix]]. For example if [[Connection::tablePrefix]] is `tbl_`,
     * `Customer` becomes `tbl_customer`, and `OrderItem` becomes `tbl_order_item`. You may override this method
     * if the table is not named after this convention.
     *
     * @return string the table name
     */
    public static function tableName() : string
    {
        return  Inflector::camel2id(StringHelper::basename(static::class), '_');
    }

    /**
     * Returns table aliases which are not the same as the name of the tables.
     *
     * @param Query $query
     *
     * @return array
     * @throws \Exception
     * @since 2.0.17
     * @internal
     */
    protected static function filterValidAliases(Query $query) : array
    {
        $tables = $query->getTablesUsedInFrom();

        $aliases = array_diff(array_keys($tables), $tables);

        return array_map(static function ($alias) {
            return preg_replace('/{{([\w]+)}}/', '$1', $alias);
        }, array_values($aliases));
    }

    /**
     * Filters array condition before it is assiged to a Query filter.
     * This method will ensure that an array condition only filters on existing table columns.
     *
     * @param array $condition condition to filter.
     * @param array $aliases
     *
     * @return array filtered condition.
     * @throws \Exception
     * @since 2.0.15
     * @internal
     */
    protected static function filterCondition(array $condition, array $aliases = []) : array
    {
        $result = [];
        $db = static::getDb();
        $columnNames = static::filterValidColumnNames($db, $aliases);

        foreach ($condition as $key => $value) {
            if (is_string($key) && !in_array($db->quoteSql($key), $columnNames, true)) {
                throw new InvalidArgumentException('Key "' . $key . '" is not a column name and can not be used as a filter');
            }
            $result[$key] = is_array($value) ? array_values($value) : $value;
        }

        return $result;
    }

    /**
     * Returns the database connection used by this AR class.
     * By default, the "db" application component is used as the database connection.
     * You may override this method if you want to use a different database connection.
     *
     * @return object|\rickcy\tarantool\Connection
     * @throws \Exception
     */
    public static function getDb()
    {
        return Yii::$container->get('tarantool');
    }

    /**
     * Valid column names are table column names or column names prefixed with table name or table alias
     *
     * @param Connection $db
     * @param array $aliases
     *
     * @return array
     * @throws \Exception
     * @since 2.0.17
     * @internal
     */
    protected static function filterValidColumnNames($db, array $aliases) : array
    {
        $columnNames = [];
        $tableName = static::tableName();
        $quotedTableName = $db->quoteTableName($tableName);

        foreach (static::getColumns() as $columnName) {
            $columnNames[] = $columnName;
            $columnNames[] = $db->quoteColumnName($columnName);
            $columnNames[] = "$tableName.$columnName";
            $columnNames[] = $db->quoteSql("$quotedTableName.[[$columnName]]");
            foreach ($aliases as $tableAlias) {
                $columnNames[] = "$tableAlias.$columnName";
                $quotedTableAlias = $db->quoteTableName($tableAlias);
                $columnNames[] = $db->quoteSql("$quotedTableAlias.[[$columnName]]");
            }
        }

        return $columnNames;
    }

    abstract public static function getColumns();

    /**
     * Inserts a row into the associated database table using the attribute values of this record.
     * This method performs the following steps in order:
     * 1. call [[beforeValidate()]] when `$runValidation` is `true`. If [[beforeValidate()]]
     *    returns `false`, the rest of the steps will be skipped;
     * 2. call [[afterValidate()]] when `$runValidation` is `true`. If validation
     *    failed, the rest of the steps will be skipped;
     * 3. call [[beforeSave()]]. If [[beforeSave()]] returns `false`,
     *    the rest of the steps will be skipped;
     * 4. insert the record into database. If this fails, it will skip the rest of the steps;
     * 5. call [[afterSave()]];
     * In the above step 1, 2, 3 and 5, events [[EVENT_BEFORE_VALIDATE]],
     * [[EVENT_AFTER_VALIDATE]], [[EVENT_BEFORE_INSERT]], and [[EVENT_AFTER_INSERT]]
     * will be raised by the corresponding methods.
     * Only the [[dirtyAttributes|changed attribute values]] will be inserted into database.
     * If the table's primary key is auto-incremental and is `null` during insertion,
     * it will be populated with the actual value after insertion.
     * For example, to insert a customer record:
     * ```php
     * $customer = new Customer;
     * $customer->name = $name;
     * $customer->email = $email;
     * $customer->insert();
     * ```
     *
     * @param bool $runValidation whether to perform validation (calling [[validate()]])
     * before saving the record. Defaults to `true`. If the validation fails, the record
     * will not be saved to the database and this method will return `false`.
     * @param array $attributes list of attributes that need to be saved. Defaults to `null`,
     * meaning all attributes that are loaded from DB will be saved.
     *
     * @return bool whether the attributes are valid and the record is inserted successfully.
     * @throws \Exception|\Throwable in case insert failed.
     */
    public function insert($runValidation = true, $attributes = null) : bool
    {
        if ($runValidation && !$this->validate($attributes)) {
            Yii::info('Model not inserted due to validation error.', __METHOD__);

            return false;
        }

        if (!$this->beforeSave(true)) {
            return false;
        }

        $values = $this->values();
        self::getDb()->getSpace(strtoupper(static::tableName()))->insert(array_values($values));


        foreach ($values as $name => $value) {
            $this->setAttribute($name, $value);
            $values[$name] = $value;
        }

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);
        $this->afterSave(true, $changedAttributes);

        return true;


    }


    /**
     * @return array
     * @throws \ReflectionException
     */
    public function values() : array
    {
        $class = new ReflectionClass($this);
        $names = [];
        foreach ($class->getProperties(\ReflectionProperty::IS_PUBLIC) as $property) {
            if (!$property->isStatic()) {
                $names[$property->getName()] = $this->{$property->getName()};
            }
        }

        return $names;
    }


    /**
     * Saves the changes to this active record into the associated database table.
     * This method performs the following steps in order:
     * 1. call [[beforeValidate()]] when `$runValidation` is `true`. If [[beforeValidate()]]
     *    returns `false`, the rest of the steps will be skipped;
     * 2. call [[afterValidate()]] when `$runValidation` is `true`. If validation
     *    failed, the rest of the steps will be skipped;
     * 3. call [[beforeSave()]]. If [[beforeSave()]] returns `false`,
     *    the rest of the steps will be skipped;
     * 4. save the record into database. If this fails, it will skip the rest of the steps;
     * 5. call [[afterSave()]];
     * In the above step 1, 2, 3 and 5, events [[EVENT_BEFORE_VALIDATE]],
     * [[EVENT_AFTER_VALIDATE]], [[EVENT_BEFORE_UPDATE]], and [[EVENT_AFTER_UPDATE]]
     * will be raised by the corresponding methods.
     * Only the [[dirtyAttributes|changed attribute values]] will be saved into database.
     * For example, to update a customer record:
     * ```php
     * $customer = Customer::findOne($id);
     * $customer->name = $name;
     * $customer->email = $email;
     * $customer->update();
     * ```
     * Note that it is possible the update does not affect any row in the table.
     * In this case, this method will return 0. For this reason, you should use the following
     * code to check if update() is successful or not:
     * ```php
     * if ($customer->update() !== false) {
     *     // update successful
     * } else {
     *     // update failed
     * }
     * ```
     *
     * @param bool $runValidation
     * @param null $attributeNames
     *
     * @return bool|int
     * @throws \Exception
     */
    public function update($runValidation = true, $attributeNames = null)
    {
        if ($runValidation && !$this->validate($attributeNames)) {
            Yii::info('Model not updated due to validation error.', __METHOD__);

            return false;
        }

        if (!$this->beforeSave(false)) {
            return false;
        }
        $values = $this->getDirtyAttributes($attributeNames);
        if (empty($values)) {
            $this->afterSave(false, $values);

            return 0;
        }
        $condition = $this->getOldPrimaryKey(true);
        // We do not check the return value of updateAll() because it's possible
        // that the UPDATE statement doesn't change anything and thus returns 0.
        $sql = self::getDb()->getQueryBuilder()->update(static::tableName(), $values, $condition, $params);
        $rows = self::getDb()->createCommand($sql)->execute();

        $changedAttributes = [];
        foreach ($values as $name => $value) {
            $changedAttributes[$name] = $this->_oldAttributes[$name] ?? null;
            $this->_oldAttributes[$name] = $value;
        }
        $this->afterSave(false, $changedAttributes);

        return $rows;

    }


    /**
     * Deletes the table row corresponding to this active record.
     * This method performs the following steps in order:
     * 1. call [[beforeDelete()]]. If the method returns `false`, it will skip the
     *    rest of the steps;
     * 2. delete the record from the database;
     * 3. call [[afterDelete()]].
     * In the above step 1 and 3, events named [[EVENT_BEFORE_DELETE]] and [[EVENT_AFTER_DELETE]]
     * will be raised by the corresponding methods.
     *
     * @throws \Exception
     */
    public function delete()
    {
        if (!$this->beforeDelete()) {
            return false;
        }
        // we do not check the return value of deleteAll() because it's possible
        // the record is already deleted in the database and thus the method will return 0
        $condition = $this->getOldPrimaryKey(true);
        $sql = static::getDb()->getQueryBuilder()->delete(static::tableName(), $condition, $params);
        $result = static::getDb()->createCommand($sql)->execute();
        $this->setOldAttributes(null);
        $this->afterDelete();

        return $result;
    }

    /**
     * Returns a value indicating whether the given active record is the same as the current one.
     * The comparison is made by comparing the table names and the primary key values of the two active records.
     * If one of the records [[isNewRecord|is new]] they are also considered not equal.
     *
     * @param self $record record to compare to
     *
     * @return bool whether the two active records refer to the same row in the same database table.
     */
    public function equals($record) : bool
    {
        if ($this->isNewRecord || $record->isNewRecord) {
            return false;
        }

        return static::tableName() === $record->tableName() && $this->getPrimaryKey() === $record->getPrimaryKey();
    }

}
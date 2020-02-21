<?php


namespace rickcy\tarantool;

class QueryBuilder extends \yii\db\sqlite\QueryBuilder
{

    /**
     * QueryBuilder constructor.
     *
     * @param $conn
     */
    public function __construct($conn)
    {
        parent::__construct($conn);
    }
}
<?php


namespace rickcy\tarantool;

use yii\db\ExpressionInterface;

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

    /**
     * @param string $table
     * @param array $columns
     * @param array $params
     * @return array
     */
    protected function prepareUpdateSets($table, $columns, $params = []): array
    {
        $sets = [];
        foreach ($columns as $name => $value) {

            $placeholder = $this->bindParam($value, $params);

            $sets[] = $this->db->quoteColumnName($name) . '=' . $placeholder;
        }
        return [$sets, $params];
    }


}
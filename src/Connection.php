<?php


namespace rickcy\tarantool;


use Exception;
use Tarantool\Client\Connection\StreamConnection;
use Tarantool\Client\Dsn;
use Tarantool\Client\Exception\RequestFailed;
use Tarantool\Client\Handler\DefaultHandler;
use Tarantool\Client\Handler\Handler;
use Tarantool\Client\Handler\MiddlewareHandler;
use Tarantool\Client\Keys;
use Tarantool\Client\Middleware\AuthenticationMiddleware;
use Tarantool\Client\Middleware\Middleware;
use Tarantool\Client\Middleware\RetryMiddleware;
use Tarantool\Client\Packer\PackerFactory;
use Tarantool\Client\Request\CallRequest;
use Tarantool\Client\Request\EvaluateRequest;
use Tarantool\Client\Request\PingRequest;
use Tarantool\Client\Schema\Criteria;
use Tarantool\Client\Schema\Space;
use Yii;
use yii\base\Component;
use yii\base\InvalidConfigException;
use yii\db\sqlite\Schema;


/**
 * @property Schema $schema
 * @property QueryBuilder $queryBuilder
 */
class Connection extends Component
{

    /**
     * @event yii\base\Event an event that is triggered after a DB connection is established
     */
    const EVENT_AFTER_OPEN = 'afterOpen';

    public $dsn;
    /**
     * @var string the username for establishing DB connection. Defaults to `null` meaning no username to use.
     */
    public $username;
    /**
     * @var string the password for establishing DB connection. Defaults to `null` meaning no password to use.
     */
    public $password;

    public $connect_timeout;
    public $socket_timeout;
    public $tcp_nodelay;
    public $persistent;
    public $max_retries;

    public $enableSavepoint = true;

    public $commandClass = Command::class;

    /** @var Handler */
    public $handler;


    public $tablePrefix = '';
    private $tableQuoteCharacter = '"';
    private $columnQuoteCharacter = '"';
    private $_quotedColumnNames;
    private $spaces;

    private $isActive = false;
    private $conn = null;
    private $_schema;

    public $enableSchemaCache = false;

    /**
     * /**
     * Creates a command for execution.
     *
     * @param string $sql the SQL statement to be executed
     * @param array $params the parameters to be bound to the SQL statement
     *
     * @return Command the DB command
     * @throws Exception
     */
    public function createCommand($sql = null, $params = []): Command
    {

        $config = ['class' => Command::class];
        if ($this->commandClass !== $config['class']) {
            $config['class'] = $this->commandClass;
        }
        $config['db'] = $this;
        $config['sql'] = $sql;
        /** @var Command $command */
        $command = Yii::createObject($config);

                                return $command->bindValues($params);

    }


    /**
     * @return bool
     */
    public function isActive(): bool
    {
        return $this->isActive;
    }

    /**
     * @param Middleware $middleware
     * @param Middleware ...$middlewares
     *
     * @return $this
     */
    public function withMiddleware(Middleware $middleware, Middleware ...$middlewares): self
    {
        $new = clone $this;
        $new->handler = MiddlewareHandler::create($new->handler, $middleware, ...$middlewares);

        return $new;
    }

    public function getHandler(): Handler
    {
        return $this->handler;
    }

    /**
     * @param $field
     *
     * @return string
     */
    public function quoteValue($field): string
    {
        return "'" . str_replace('`', '"', $field) . "'";
    }

    public function ping()
    {
        $this->handler->handle(new PingRequest());
    }

    /**
     * Processes a SQL statement by quoting table and column names that are enclosed within double brackets.
     * Tokens enclosed within double curly brackets are treated as table names, while
     * tokens enclosed within double square brackets are column names. They will be quoted accordingly.
     * Also, the percentage character "%" at the beginning or ending of a table name will be replaced
     * with [[tablePrefix]].
     *
     * @param string $sql the SQL to be quoted
     *
     * @return string the quoted SQL
     */
    public function quoteSql($sql): string
    {
        return preg_replace_callback(
            '/({\\{(%?[\w\-\. ]+%?)\\}\\}|\\[\\[([\w\-\. ]+)\\]\\])/',
            function ($matches) {
                if (isset($matches[3])) {
                    return $this->quoteColumnName($matches[3]);
                }

                return str_replace('%', $this->tablePrefix, $this->quoteTableName($matches[2]));
            },
            $sql
        );
    }

    /**
     * Quotes a column name for use in a query.
     * If the column name contains prefix, the prefix will also be properly quoted.
     * If the column name is already quoted or contains special characters including '(', '[[' and '{{',
     * then this method will do nothing.
     *
     * @param string $name column name
     *
     * @return string the properly quoted column name
     */
    public function quoteColumnName($name): string
    {
        if (isset($this->_quotedColumnNames[$name])) {
            return $this->_quotedColumnNames[$name];
        }

        return $this->_quotedColumnNames[$name] = $this->quoteColumnNameSchema($name);
    }

    /**
     * Quotes a column name for use in a query.
     * If the column name contains prefix, the prefix will also be properly quoted.
     * If the column name is already quoted or contains '(', '[[' or '{{',
     * then this method will do nothing.
     *
     * @param string $name column name
     *
     * @return string the properly quoted column name
     * @see quoteSimpleColumnName()
     */
    public function quoteColumnNameSchema($name): string
    {
        if (strpos($name, '(') !== false || strpos($name, '[[') !== false) {
            return $name;
        }
        if (($pos = strrpos($name, '.')) !== false) {
            $prefix = $this->quoteTableName(substr($name, 0, $pos)) . '.';
            $name = substr($name, $pos + 1);
        } else {
            $prefix = '';
        }
        if (strpos($name, '{{') !== false) {
            return $name;
        }

        return $prefix . $this->quoteSimpleColumnName($name);
    }

    /**
     * Quotes a table name for use in a query.
     * If the table name contains schema prefix, the prefix will also be properly quoted.
     * If the table name is already quoted or contains '(' or '{{',
     * then this method will do nothing.
     *
     * @param string $name table name
     *
     * @return string the properly quoted table name
     * @see quoteSimpleTableName()
     */
    public function quoteTableName($name): string
    {
        if (strpos($name, '(') !== false || strpos($name, '{{') !== false) {
            return $name;
        }
        if (strpos($name, '.') === false) {
            return $this->quoteSimpleTableName($name);
        }
        $parts = $this->getTableNameParts($name);
        foreach ($parts as $i => $part) {
            $parts[$i] = $this->quoteSimpleTableName($part);
        }

        return implode('.', $parts);
    }

    /**
     * Quotes a simple table name for use in a query.
     * A simple table name should contain the table name only without any schema prefix.
     * If the table name is already quoted, this method will do nothing.
     *
     * @param string $name table name
     *
     * @return string the properly quoted table name
     */
    public function quoteSimpleTableName($name): string
    {
        if (is_string($this->tableQuoteCharacter)) {
            $startingCharacter = $endingCharacter = $this->tableQuoteCharacter;
        } else {
            list($startingCharacter, $endingCharacter) = $this->tableQuoteCharacter;
        }

        return strpos($name, $startingCharacter) !== false ? strtoupper($name) : $startingCharacter . strtoupper($name) . $endingCharacter;
    }

    /**
     * Splits full table name into parts
     *
     * @param string $name
     *
     * @return array
     * @since 2.0.22
     */
    protected function getTableNameParts($name): array
    {
        return explode('.', $name);
    }

    /**
     * Quotes a simple column name for use in a query.
     * A simple column name should contain the column name only without any prefix.
     * If the column name is already quoted or is the asterisk character '*', this method will do nothing.
     *
     * @param string $name column name
     *
     * @return string the properly quoted column name
     */
    public function quoteSimpleColumnName($name): string
    {
        if (is_string($this->tableQuoteCharacter)) {
            $startingCharacter = $endingCharacter = $this->columnQuoteCharacter;
        } else {
            list($startingCharacter, $endingCharacter) = $this->columnQuoteCharacter;
        }

        return $name === '*' || strpos($name, $startingCharacter) !== false ? $name : $startingCharacter . $name . $endingCharacter;
    }

    /**
     * @return QueryBuilder
     */
    public function getQueryBuilder(): QueryBuilder
    {
        $this->open();
        return new QueryBuilder($this->conn);
    }

    /**
     * @return object
     * @throws InvalidConfigException
     */
    public function getSchema()
    {
        if ($this->_schema !== null) {
            return $this->_schema;
        }

        $config = ['class' => Schema::class];
        $config['db'] = $this;

        return $this->_schema = Yii::createObject($config);

    }


    public function evaluate(string $expr, ...$args): array
    {
        $this->open();
        $request = new EvaluateRequest($expr, $args);

        return $this->handler->handle($request)->getBodyField(Keys::DATA);
    }

    public function call(string $funcName, ...$args): array
    {
        $this->open();
        $request = new CallRequest($funcName, $args);

        return $this->handler->handle($request)->getBodyField(Keys::DATA);
    }

    /**
     * @param string $spaceName
     *
     * @return Space
     */
    public function getSpace(string $spaceName): Space
    {
        $this->open();
        if (isset($this->spaces[$spaceName])) {
            return $this->spaces[$spaceName];
        }

        $spaceId = $this->getSpaceIdByName($spaceName);

        return $this->spaces[$spaceName] = $this->spaces[$spaceId] = new Space($this->handler, $spaceId);
    }

    /**
     * @return object|Connection
     * @throws InvalidConfigException
     * @throws Exception
     */
    public function open()
    {
        if ($this->isActive) {
            return $this->conn ?? Yii::$container->get('tarantool')->open();
        }
        $dsn = Dsn::parse($this->dsn);

        $connectionOptions = [];

        if ($connect_timeout = $this->connect_timeout ?? $dsn->getInt('connect_timeout')) {
            $connectionOptions['connect_timeout'] = $connect_timeout;
        }
        if ($socket_timeout = $this->socket_timeout ?? $dsn->getInt('socket_timeout')) {
            $connectionOptions['socket_timeout'] = $socket_timeout;
        }
        if ($tcp_nodelay = $this->tcp_nodelay ?? $dsn->getBool('tcp_nodelay')) {
            $connectionOptions['tcp_nodelay'] = $tcp_nodelay;
        }
        if ($persistent = $this->persistent ?? $dsn->getBool('persistent')) {
            $connectionOptions['persistent'] = $persistent;
        }

        $connection = $dsn->isTcp()
            ? StreamConnection::createTcp($dsn->getConnectionUri(), $connectionOptions)
            : StreamConnection::createUds($dsn->getConnectionUri(), $connectionOptions);

        $handler = new DefaultHandler($connection, PackerFactory::create());

        if ($maxRetries = $this->max_retries ?? $dsn->getInt('max_retries')) {
            $handler = MiddlewareHandler::create($handler, RetryMiddleware::linear($maxRetries));
        }
        if ($username = $this->username ?? $dsn->getUsername()) {
            $password = $this->password ?? $dsn->getPassword();
            $this->handler = MiddlewareHandler::create($handler, new AuthenticationMiddleware($username, $password ?? ''));
        }

        $this->trigger(self::EVENT_AFTER_OPEN);
        $this->isActive = true;

        /** @var Connection conn */
        return $this->conn = Yii::createObject([
            'class' => static::class,
            'dsn' => 'tcp://175.25.125.7:3301',
            'username' => 'tester',
            'password' => 'test',
        ]);

    }

    /**
     * @param string $spaceName
     *
     * @return int
     */
    private function getSpaceIdByName(string $spaceName): int
    {
        $schema = $this->getSpaceById(Space::VSPACE_ID);
        $data = $schema->select(Criteria::key([$spaceName])->andIndex(Space::VSPACE_NAME_INDEX));

        if ([] === $data) {
            throw RequestFailed::unknownSpace($spaceName);
        }

        return $data[0][0];
    }

    public function getSpaceById(int $spaceId): Space
    {
        if (isset($this->spaces[$spaceId])) {
            return $this->spaces[$spaceId];
        }

        return $this->spaces[$spaceId] = new Space($this->handler, $spaceId);
    }
}
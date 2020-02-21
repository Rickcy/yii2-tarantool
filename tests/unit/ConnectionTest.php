<?php


namespace rickcy\tarantool\tests\unit;


use PHPUnit\Framework\TestCase;
use rickcy\tarantool\Connection;
use rickcy\tarantool\tests\User;
use Tarantool\Client\Schema\Criteria;

class ConnectionTest extends TestCase
{
    /**
     * @throws \Exception
     */
    public function testConnection()
    {
        $connection = new Connection();
        $connection->dsn = 'tcp://185.45.0.100:3301';
        $connection->username = 'tester';
        $connection->password = 'test';
        $connection->socket_timeout = 20000;
        $connection->connect_timeout = 20000;
        $connection->persistent = true;

        $user = $connection->getSpace('USERS')->select(Criteria::key([13454]))[0];

        $this->assertTrue(true);


    }


    public function testActiveRecord()
    {

        $container = \Yii::$container;
        $container->setDefinitions([
            'tarantool' =>
                [
                    'class' => Connection::class,
                    'dsn' => 'tcp://185.45.0.100:3301',
                    'username' => 'tester',
                    'password' => 'test',
                    'persistent' => true,
                ],
        ]);

        $user = new User();
        $user->id = null;
        $user->guid = '1fb4154db-76b2-11e7-bc8d-901b0ebda105';
        $user->email = 'kuden.and.ko@gmail.com';
        $user->name = 'Евгений';
        $user->surname = 'Куденко';
        $user->middle__name = 'Сергеевич';

        $user->save();

    }
}